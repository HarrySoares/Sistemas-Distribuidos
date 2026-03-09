import os
import json
from collections import Counter

ARQUIVO_LOG = "market_logs.json"

print("🚀 Iniciando Motor de Analytics (Modo Standalone / Batch)...")

if not os.path.exists(ARQUIVO_LOG):
    print("❌ Nenhum dado encontrado. Faça uma compra no cliente primeiro para gerar o log.")
else:
    try:
        # Lê os logs gerados pelo Servidor de Transações
        with open(ARQUIVO_LOG, "r", encoding="utf-8") as f:
            logs = [json.loads(linha) for linha in f if linha.strip()]
        
        # Filtra apenas as vendas reais
        vendas = [log for log in logs if log.get("tipo_evento") == "VENDA"]
        
        print("\n" + "="*55)
        print(" 📊 RELATÓRIO DE INTELIGÊNCIA DE MERCADO (TRADEHUB)")
        print("="*55)
        
        # 1. Total de vendas
        total_vendas = len(vendas)
        print(f"📈 Total de Skins Vendidas: {total_vendas}")
        
        # 2. Soma do faturamento
        receita_total = sum(log.get("valor", 0) for log in vendas)
        print(f"💰 Volume Financeiro Total: R$ {receita_total:,.2f}")
        
        # 3. Ranking das skins mais vendidas
        print("\n🏆 Ranking de Popularidade (Skins mais compradas):")
        contador_itens = Counter(log.get("item") for log in vendas)
        for item, qtd in contador_itens.most_common():
            print(f"- {item:<25} | {qtd} venda(s)")
            
    except Exception as e:
        print(f"🚨 ERRO NA EXECUÇÃO: {e}")

print("\n✅ Processamento concluído com sucesso.")