import socket
import threading
import json

# O Servidor de Nomes atua como o Service Discovery (DNS fixo da arquitetura)
HOST = '0.0.0.0' # 0.0.0.0 significa "Escute em todas as placas de rede deste PC"
PORTA_NOME = 9000 

# "Lista telefônica" em memória que guarda onde cada serviço está rodando
REGISTRO_SERVICOS = {}
lock_registro = threading.Lock() # Exclusão Mútua local para proteger o dicionário

def manipular_requisicao(conn, endereco):
    try:
        msg_bytes = conn.recv(4096)
        if msg_bytes:
            # Lendo o Envelope JSON padrão da arquitetura
            envelope = json.loads(msg_bytes.decode('utf-8'))
            tipo_msg = envelope.get("header", {}).get("tipo_mensagem")
            payload = envelope.get("payload", {})

            # CASO 1: Um servidor de mercado quer se registrar
            if tipo_msg == "COMANDO_REGISTRAR_SERVICO":
                nome = payload.get("nome_servico")
                ip = payload.get("ip")
                porta = payload.get("porta")
                
                with lock_registro:
                    REGISTRO_SERVICOS[nome] = {"ip": ip, "porta": porta}
                
                print(f"[REGISTRO] '{nome}' registrado em {ip}:{porta}")
                conn.send(json.dumps({"status": "OK"}).encode('utf-8'))

            # CASO 2: Um cliente quer descobrir onde o mercado está (Transparência de Localização)
            elif tipo_msg == "COMANDO_DESCOBRIR_SERVICO":
                nome = payload.get("nome_servico")
                
                with lock_registro:
                    if nome in REGISTRO_SERVICOS:
                        resposta = {
                            "status": "ENCONTRADO", 
                            "ip": REGISTRO_SERVICOS[nome]["ip"], 
                            "porta": REGISTRO_SERVICOS[nome]["porta"]
                        }
                    else:
                        resposta = {"status": "NAO_ENCONTRADO"}
                        
                print(f"[CONSULTA] Consultaram por '{nome}'. Resultado: {resposta['status']}")
                conn.send(json.dumps(resposta).encode('utf-8'))

    except Exception as e: 
        print(f"Erro no processamento do DNS: {e}")
    finally: 
        # Padrão Request-Reply: A conexão é fechada logo após a resposta
        conn.close()

def iniciar_servidor_nomes():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((HOST, PORTA_NOME))
    servidor.listen()
    
    print(f"--- 🗺️  SERVIDOR DE NOMES ON-LINE ({HOST}:{PORTA_NOME}) ---")

    while True:
        conn, endereco = servidor.accept()
        # Concorrência: Delega o atendimento rápido para uma Thread independente
        threading.Thread(target=manipular_requisicao, args=(conn, endereco)).start()

if __name__ == "__main__":
    iniciar_servidor_nomes()