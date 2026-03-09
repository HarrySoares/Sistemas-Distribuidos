import socket
import json
import time
import uuid
import threading
from datetime import datetime, timezone

CLIENT_ID = f"usr_{str(uuid.uuid4())[:4]}"

# Variáveis do Relógio de Lamport
RELOGIO_LAMPORT = 0
lock_relogio = threading.Lock()

def tick_lamport():
    global RELOGIO_LAMPORT
    with lock_relogio:
        RELOGIO_LAMPORT += 1
        return RELOGIO_LAMPORT

def sync_lamport(tempo_recebido):
    global RELOGIO_LAMPORT
    with lock_relogio:
        RELOGIO_LAMPORT = max(RELOGIO_LAMPORT, tempo_recebido) + 1
        return RELOGIO_LAMPORT

# Variáveis para a Exclusão Mútua (Ricart-Agrawala)
ESTADO_CS = "RELEASED" 
meu_timestamp_pedido = 0
fila_pendentes = []
peers_ativos = []
respostas_faltantes = 0
evento_cs = threading.Event() 
lock_cs = threading.Lock()

def criar_envelope(tipo_mensagem, payload):
    # Padroniza as mensagens e já embute o relógio de Lamport
    return {
        "header": {
            "msg_id": str(uuid.uuid4()), "timestamp": datetime.now(timezone.utc).isoformat(),
            "remetente_id": CLIENT_ID, "tipo_mensagem": tipo_mensagem, "relogio_lamport": tick_lamport()
        },
        "payload": payload
    }

def descobrir_servidor():
    print("🔍 Procurando Servidor TradeHub no DNS (Porta 9000)...")
    IP_DO_DNS = '127.0.0.1' 
    try:
        dns_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # ANTES ESTAVA ASSIM: dns_socket.connect(('127.0.0.1', 9000))
        # DEIXE ASSIM:
        dns_socket.connect((IP_DO_DNS, 9000)) 
        
        dns_socket.send(json.dumps({"header": {"tipo_mensagem": "COMANDO_DESCOBRIR_SERVICO"}, "payload": {"nome_servico": "servidor_tradehub"}}).encode('utf-8'))
        resposta = json.loads(dns_socket.recv(4096).decode('utf-8'))
        dns_socket.close()
        if resposta.get("status") == "ENCONTRADO": return resposta["ip"], resposta["porta"]
        return None, None
    except: return None, None

def escutar_servidor(client_socket):
    global ESTADO_CS, fila_pendentes, respostas_faltantes, peers_ativos
    
    while True:
        try:
            msg_bytes = client_socket.recv(4096)
            if not msg_bytes: break
            
            resposta = json.loads(msg_bytes.decode('utf-8'))
            header = resposta.get("header", {})
            payload = resposta.get("payload", {})
            tipo_msg = header.get("tipo_mensagem")
            remetente = header.get("remetente_id")
            
            # Sincroniza o relógio sempre que recebe algo
            sync_lamport(header.get("relogio_lamport", 0))

            if tipo_msg == "EVENTO_ATUALIZACAO_PEERS":
                peers_ativos = payload.get("peers", [])
                if CLIENT_ID in peers_ativos: peers_ativos.remove(CLIENT_ID) 

            # Trata as mensagens P2P de negociação da seção crítica
            elif tipo_msg == "MENSAGEM_P2P":
                sub_tipo = payload.get("tipo_p2p")
                origem_p2p = payload.get("origem")

                # Alguém pediu para comprar
                if sub_tipo == "REQ_CS":
                    ts_req = payload.get("timestamp_pedido")
                    with lock_cs:
                        # Define quem tem prioridade com base no Lamport
                        eu_tenho_prioridade = (ESTADO_CS == "HELD") or (ESTADO_CS == "WANTED" and (meu_timestamp_pedido < ts_req or (meu_timestamp_pedido == ts_req and CLIENT_ID < origem_p2p)))
                        
                        if eu_tenho_prioridade:
                            fila_pendentes.append(origem_p2p)
                            print(f"\n🔒 [MUTEX] Segurei o pedido de {origem_p2p}. Eu tenho prioridade!\n>> ", end="", flush=True)
                        else:
                            resp = criar_envelope("ROTEAR_P2P", {"destino": origem_p2p, "tipo_p2p": "REPLY_CS", "origem": CLIENT_ID})
                            client_socket.send(json.dumps(resp).encode('utf-8'))

                # Recebi o OK de alguém
                elif sub_tipo == "REPLY_CS":
                    with lock_cs:
                        respostas_faltantes -= 1
                        # Se todo mundo liberou, destrava a thread principal
                        if respostas_faltantes <= 0 and ESTADO_CS == "WANTED":
                            evento_cs.set() 

            elif tipo_msg == "RESPOSTA_CATALOGO":
                print(f"\n{'--- CATÁLOGO ---':^30}")
                for skin, preco in payload.get('itens', {}).items(): print(f"{skin:<20} | R$ {preco:,.2f}")
                print("\n>> ", end="")
                    
            elif tipo_msg == "RECIBO_TRANSACAO":
                print(f"\n✅ SUCESSO: {payload.get('motivo')}\n>> ", end="") if payload.get("status") == "SUCESSO" else print(f"\n❌ FALHA: {payload.get('motivo')}\n>> ", end="")
            
            elif tipo_msg == "EVENTO_ITEM_VENDIDO":
                print(f"\n📢 [VENDIDO] '{payload.get('comprador')}' comprou a '{payload.get('item')}'!\n>> ", end="", flush=True)

            elif tipo_msg == "EVENTO_FLUTUACAO_PRECO":
                icone = "📈" if payload.get('tendencia') == "alta" else "📉"
                print(f"\n{icone} [TICKER] '{payload.get('item')}' > R$ {payload.get('novo_preco'):,.2f}!\n>> ", end="", flush=True)

        except Exception: break

def iniciar_cliente():
    global ESTADO_CS, meu_timestamp_pedido, respostas_faltantes, fila_pendentes
    ip_alvo, porta_alvo = descobrir_servidor()
    if not ip_alvo: return

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((ip_alvo, porta_alvo))
        client.send(json.dumps(criar_envelope("REGISTRAR_CLIENTE", {})).encode('utf-8'))
        threading.Thread(target=escutar_servidor, args=(client,), daemon=True).start()
        
        while True:
            time.sleep(0.5) 
            print(f"\n=== TRADEHUB [{CLIENT_ID}] (Lamport: {RELOGIO_LAMPORT}) ===")
            print("1. Ver Skins | 2. Comprar Skin | 3. Sair")
            opcao = input(">> ")

            if opcao == '1':
                client.send(json.dumps(criar_envelope("COMANDO_SOLICITAR_CATALOGO", {})).encode('utf-8'))
            
            elif opcao == '2':
                nome_item = input("Qual o nome EXATO da skin? ")
                
                # Inicia o processo para entrar na Seção Crítica
                with lock_cs:
                    ESTADO_CS = "WANTED"
                    meu_timestamp_pedido = tick_lamport()
                    respostas_faltantes = len(peers_ativos)
                    evento_cs.clear()

                if respostas_faltantes > 0:
                    print(f"⏳ [MUTEX] Pedindo permissão para a rede (Lamport: {meu_timestamp_pedido})...")
                    req = criar_envelope("ROTEAR_P2P", {"destino": "TODOS", "tipo_p2p": "REQ_CS", "origem": CLIENT_ID, "timestamp_pedido": meu_timestamp_pedido})
                    client.send(json.dumps(req).encode('utf-8'))
                    evento_cs.wait() # Fica aguardando até os peers responderem
                
                print("🟢 [MUTEX] Todos liberaram! O recurso é meu.")
                with lock_cs: ESTADO_CS = "HELD"
                
                # Efetua a compra
                client.send(json.dumps(criar_envelope("COMANDO_INTENCAO_COMPRA", {"item_id": nome_item})).encode('utf-8'))
                time.sleep(1) 
                
                # Terminou a compra, libera o recurso para os outros
                with lock_cs:
                    ESTADO_CS = "RELEASED"
                    for peer in fila_pendentes:
                        resp = criar_envelope("ROTEAR_P2P", {"destino": peer, "tipo_p2p": "REPLY_CS", "origem": CLIENT_ID})
                        client.send(json.dumps(resp).encode('utf-8'))
                    fila_pendentes.clear()

            elif opcao == '3':
                client.send(json.dumps(criar_envelope("COMANDO_SAIR", {})).encode('utf-8'))
                break

    finally: client.close()

if __name__ == "__main__": iniciar_cliente()