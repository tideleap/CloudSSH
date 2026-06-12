import { Env, SSHConnectionConfig } from '../types';
import { SSHSession } from './ssh-session';

export class SSHSessionDO {
  private state: DurableObjectState;
  private env: Env;
  private sessions: Map<WebSocket, SSHSession> = new Map();

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    console.log('[DO] fetch called');
    
    const upgradeHeader = request.headers.get('Upgrade');
    if (upgradeHeader !== 'websocket') {
      console.log('[DO] Not a WebSocket request');
      return new Response('Expected WebSocket', { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];

    (server as any).accept();
    console.log('[DO] WebSocket accepted, waiting for credentials...');

    this.waitForCredentials(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    } as any);
  }

  private waitForCredentials(ws: WebSocket): void {
    console.log('[DO] Setting up credential listener');
    
    const timeout = setTimeout(() => {
      console.log('[DO] Credential timeout');
      ws.send(JSON.stringify({ type: 'error', message: 'Connection timeout' }));
      ws.close(1011, 'Timeout');
    }, 10000);

    const handler = (event: MessageEvent) => {
      console.log('[DO] Received WebSocket message');
      clearTimeout(timeout);
      ws.removeEventListener('message', handler);
      
      try {
        const data = event.data;
        console.log('[DO] Message type:', typeof data, 'length:', typeof data === 'string' ? data.length : 'N/A');
        
        const config = JSON.parse(data as string) as SSHConnectionConfig;
        console.log('[DO] Parsed config - host:', config.host, 'port:', config.port, 'user:', config.username);
        
        if (!config.host || !config.username || !config.password) {
          console.log('[DO] Missing credentials');
          ws.send(JSON.stringify({ type: 'error', message: 'Missing credentials' }));
          ws.close(1011, 'Invalid credentials');
          return;
        }

        console.log('[DO] Credentials valid, initializing SSH session...');
        this.initSSHSession(ws, config);
      } catch (e) {
        console.error('[DO] Parse error:', e);
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid credentials format' }));
        ws.close(1011, 'Invalid format');
      }
    };

    ws.addEventListener('message', handler);
    ws.addEventListener('close', () => {
      console.log('[DO] WebSocket closed before credentials');
      clearTimeout(timeout);
    });
    ws.addEventListener('error', (e) => {
      console.error('[DO] WebSocket error:', e);
    });
  }

  private async initSSHSession(
    ws: WebSocket,
    config: SSHConnectionConfig
  ): Promise<void> {
    try {
      console.log('[SSH] Creating TCP connection to', config.host, config.port);
      const { connect } = await import('cloudflare:sockets');
      const socket = connect({ hostname: config.host, port: config.port });
      
      console.log('[SSH] Waiting for TCP connection...');
      await socket.opened;
      console.log('[SSH] TCP connected successfully');

      console.log('[SSH] Creating SSH session...');
      const session = new SSHSession(ws, socket, config);
      this.sessions.set(ws, session);

      ws.addEventListener('message', (event) => {
        console.log('[WS] Received user input');
        session.handleWebSocketMessage(event.data);
      });

      ws.addEventListener('close', () => {
        console.log('[WS] Connection closed');
        session.close();
        this.sessions.delete(ws);
      });

      ws.addEventListener('error', (e) => {
        console.error('[WS] Error:', e);
        session.close();
        this.sessions.delete(ws);
      });

      console.log('[SSH] Starting handshake...');
      await session.startHandshake();
      console.log('[SSH] Handshake initiated');

    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);
      const errStack = error instanceof Error ? error.stack : '';
      console.error('[SSH] Session error:', errMsg, errStack);
      ws.send(JSON.stringify({ type: 'error', message: `连接失败: ${errMsg}` }));
      ws.close(1011, 'SSH connection failed');
    }
  }
}
