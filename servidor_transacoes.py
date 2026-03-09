import socket
import threading
import json
import uuid
import time
import random
from datetime import datetime, timezone
import sys


        
#banco de dados skins
MERCADO_SKINS = {
   "AK-47 | Redline": 120.50,
    "AK-47 | Asiimov": 450.00,
    "AK-47 | Case Hardened": 1200.00,
    "M4A4 | Howl": 15000.00,
    "M4A1-S | Hyper Beast": 350.00,
    "AWP | Dragon Lore": 25000.00,
    "AWP | Medusa": 9000.00,
    "AWP | Neo-Noir": 180.00,
    "Glock-18 | Fade": 3000.00,
    "USP-S | Kill Confirmed": 600.00,
    "Desert Eagle | Blaze": 3500.00,
    "Karambit | Doppler": 5500.00,
    "Butterfly Knife | Fade": 11000.00,
    "M9 Bayonet | Marble Fade": 7200.00
    }

MAPA_CLIENTES = {} # Mapeia ID do Cliente -> Conexão Socket
lock_clientes = threading.Lock()
lock_mercado = threading.Lock() 

# Relógio Lógico de Lamport (Mantém a ordem causal dos eventos)
RELOGIO_LAMPORT = 0
lock_relogio = threading.Lock()

def registrar_log_analytics(tipo_evento, dados):
    """Grava logs em formato JSONL (JSON Lines) para o Apache Spark processar"""
    log_entry = {
        "timestamp_log": datetime.now(timezone.utc).isoformat(),
        "tipo_evento": tipo_evento
    }
    log_entry.update(dados) # Junta o tipo de evento com os dados da skin/compra
    
    with open("market_logs.json", "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry) + "\n")

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

def criar_envelope(tipo_mensagem, payload):
    # O Envelope padroniza a comunicação e carrega o Relógio de Lamport embutido
    return {
        "header": {
            "msg_id": str(uuid.uuid4()), "timestamp": datetime.now(timezone.utc).isoformat(),
            "remetente_id": "srv_mercado", "tipo_mensagem": tipo_mensagem, "relogio_lamport": tick_lamport()
        },
        "payload": payload
    }

def broadcast_peers():
    # Atualiza a rede P2P: informa a todos quem está online no momento
    with lock_clientes:
        lista = list(MAPA_CLIENTES.keys())
        msg = json.dumps(criar_envelope("EVENTO_ATUALIZACAO_PEERS", {"peers": lista})).encode('utf-8')
        for conn in MAPA_CLIENTES.values():
            try: conn.send(msg)
            except: pass

def simulador_bolsa_de_valores():
    # Fluxo contínuo de dados (Data Stream): Envia eventos assíncronos de oscilação de preço
    while True:
        time.sleep(60) 
        with lock_mercado:
            if not MERCADO_SKINS: continue
            item_sorteado = random.choice(list(MERCADO_SKINS.keys()))
            variacao = random.uniform(-0.05, 0.05)
            MERCADO_SKINS[item_sorteado] = round(MERCADO_SKINS[item_sorteado] * (1 + variacao), 2)
            preco_atual = MERCADO_SKINS[item_sorteado]
            
        msg_bytes = json.dumps(criar_envelope("EVENTO_FLUTUACAO_PRECO", {"item": item_sorteado, "novo_preco": preco_atual, "tendencia": "alta" if variacao > 0 else "baixa"})).encode('utf-8')
        with lock_clientes:
            for conn in MAPA_CLIENTES.values():
                try: conn.send(msg_bytes)
                except: pass

def manipular_cliente(conn, endereco):
    conectado = True
    meu_id_cliente = None
    
    while conectado:
        try:
            msg_bytes = conn.recv(4096)
            if not msg_bytes: break 
            
            req = json.loads(msg_bytes.decode('utf-8'))
            header = req.get("header", {})
            payload = req.get("payload", {})
            tipo_msg = header.get("tipo_mensagem")
            remetente = header.get("remetente_id")
            
            # Sincroniza o relógio do servidor com o do cliente
            sync_lamport(header.get("relogio_lamport", 0))
            
            if tipo_msg == "REGISTRAR_CLIENTE":
                meu_id_cliente = remetente
                with lock_clientes: MAPA_CLIENTES[meu_id_cliente] = conn
                broadcast_peers()

            # ROTEADOR P2P: Repassa as mensagens de Exclusão Mútua (Ricart-Agrawala) entre clientes
            elif tipo_msg == "ROTEAR_P2P":
                destino = payload.get("destino")
                msg_p2p = json.dumps(criar_envelope("MENSAGEM_P2P", payload)).encode('utf-8')
                with lock_clientes:
                    if destino == "TODOS":
                        for c_id, c_conn in MAPA_CLIENTES.items():
                            if c_id != remetente: c_conn.send(msg_p2p)
                    elif destino in MAPA_CLIENTES:
                        MAPA_CLIENTES[destino].send(msg_p2p)

            elif tipo_msg == "COMANDO_SOLICITAR_CATALOGO":
                conn.send(json.dumps(criar_envelope("RESPOSTA_CATALOGO", {"itens": MERCADO_SKINS})).encode('utf-8'))
            
            elif tipo_msg == "COMANDO_INTENCAO_COMPRA":
                item_id = payload.get("item_id")
                
                # O servidor confia que os clientes já negociaram a trava
                if item_id in MERCADO_SKINS:
                    preco_vendido = MERCADO_SKINS[item_id] # 1. SALVA O PREÇO ANTES DE DELETAR
                    del MERCADO_SKINS[item_id] 
                    conn.send(json.dumps(criar_envelope("RECIBO_TRANSACAO", {"status": "SUCESSO", "motivo": f"Você comprou '{item_id}'!"})).encode('utf-8'))
                    
                    # 2. CHAMA A FUNÇÃO DE LOG AQUI PARA O SPARK LER DEPOIS
                    registrar_log_analytics("VENDA", {"item": item_id, "comprador": remetente, "valor": preco_vendido})
                    
                    #  Evento de Broadcast avisando a venda
                    msg_venda = json.dumps(criar_envelope("EVENTO_ITEM_VENDIDO", {"item": item_id, "comprador": remetente})).encode('utf-8')
                    with lock_clientes:
                        for c_id, c_conn in MAPA_CLIENTES.items():
                            if c_id != remetente: c_conn.send(msg_venda)
                else:
                    conn.send(json.dumps(criar_envelope("RECIBO_TRANSACAO", {"status": "ERRO", "motivo": f"Skin indisponível."})).encode('utf-8'))
            elif tipo_msg == "COMANDO_SAIR": conectado = False

        except Exception: break

    # Trata a desconexão e avisa a rede P2P
    if meu_id_cliente:
        with lock_clientes: 
            if meu_id_cliente in MAPA_CLIENTES: del MAPA_CLIENTES[meu_id_cliente]
        broadcast_peers()
    conn.close()

def obter_ip_local():
    """Função auxiliar para descobrir o próprio IP na rede Wi-Fi/Cabo"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def iniciar_servidor():
    IP_DO_DNS = '127.0.0.1' 
    
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    meu_ip = obter_ip_local()
    servidor.bind((meu_ip, 0)) 
    minha_porta = servidor.getsockname()[1]
    
    try:
        dns_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        dns_socket.connect((IP_DO_DNS, 9000)) 
        dns_socket.send(json.dumps({"header": {"tipo_mensagem": "COMANDO_REGISTRAR_SERVICO"}, "payload": {"nome_servico": "servidor_tradehub", "ip": meu_ip, "porta": minha_porta}}).encode('utf-8'))
        dns_socket.close()
    except Exception:
        print("[!] ERRO: Servidor de Nomes não encontrado.")
        sys.exit()
        
    servidor.listen()
    print(f"--- 💰 SERVIDOR TRADEHUB ON-LINE ({meu_ip}:{minha_porta}) ---")
    threading.Thread(target=simulador_bolsa_de_valores, daemon=True).start()

    while True:
        conn, endereco = servidor.accept()
        threading.Thread(target=manipular_cliente, args=(conn, endereco)).start()

if __name__ == "__main__":
    iniciar_servidor()