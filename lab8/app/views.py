from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import time
import requests
import json
from concurrent import futures

# Настройки
# Убедись, что порт 8090 совпадает с тем, на котором запущен твой Go сервер
GO_SERVICE_URL = "http://localhost:8090/api/internal/msghistory/updating" 
AUTH_TOKEN = "secret12" # Токен, который ожидает Go (h.SetMsghistoryResult)

# Пул потоков для асинхронности (чтобы сразу отвечать 200 OK)
executor = futures.ThreadPoolExecutor(max_workers=1)

def calculate_logic(data):
    """
    Асинхронный расчет метрик Msghistory.
    Повторяет логику Go-функции postAnalysis.
    """
    req_id = data.get('id')
    print(f"Start processing Msghistory ID: {req_id}")
    
    # 1. Имитация задержки (можно убрать в проде, но для лабы пусть будет)
    time.sleep(15)

    try:
        items = data.get('items', [])
        
        total_eff_views = 0.0    # ∑ min(views, subscribers)
        total_subscribers = 0.0  # ∑ subscribers по уникальным каналам

        root_eff_views = 0.0     # уровень 0
        repost_eff_views = 0.0   # уровни > 0

        seen_channels = set()    # аналог seen := make(map[uint]struct{})

        for item in items:
            channel_id = item.get('channel_id')
            
            # --- Извлекаем просмотры (Go: link.Views)
            # Если None, считаем 0
            views_val = item.get('views')
            views_f = float(views_val) if views_val is not None else 0.0

            # --- Извлекаем подписчиков (Go: link.Channel.Subscribers)
            subs_val = item.get('subscribers')
            
            # --- Эффективные просмотры: min(views, subscribers)
            eff_views = views_f
            if subs_val is not None:
                subs_f = float(subs_val)
                if subs_f < eff_views:
                    eff_views = subs_f
            
            total_eff_views += eff_views

            # --- Суммируем подписчиков по уникальным каналам
            # (Go: if _, ok := seen[link.ChannelID]; !ok)
            if channel_id is not None and channel_id not in seen_channels:
                if subs_val is not None:
                    total_subscribers += float(subs_val)
                seen_channels.add(channel_id)

            # --- Распределение по уровням для Retention Rate
            # (Go: level := *link.RepostLevel)
            level_val = item.get('repost_level')
            level = int(level_val) if level_val is not None else 0

            if level == 0:
                root_eff_views += eff_views
            else:
                repost_eff_views += eff_views

        # --- Финальные формулы ---
        
        # 1. Coverage % = (totalEffViews / totalSubscribers) * 100
        coverage = 0.0
        if total_subscribers > 0:
            coverage = (total_eff_views / total_subscribers) * 100.0

        # 2. Coefficient (Retention) = repostEffViews / rootEffViews
        coefficient = 0.0
        if root_eff_views > 0:
            coefficient = repost_eff_views / root_eff_views

        # Округлим для красоты (не обязательно)
        coverage = round(coverage, 4)
        coefficient = round(coefficient, 4)

        print(f"Calculated ID {req_id}: Coverage={coverage}, Coeff={coefficient}")

        # 3. Отправка результата обратно в Go
        # Поля должны совпадать с JSON тегами структуры AsyncCalcResponse в Go
        result_payload = {
            "id": req_id,
            "coverage": coverage,
            "coefficient": coefficient
        }
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": AUTH_TOKEN 
        }

        # Отправляем PUT запрос на Go backend
        resp = requests.put(GO_SERVICE_URL, json=result_payload, headers=headers, timeout=5)
        
        if resp.status_code == 200:
            print(f"Successfully updated Go service for ID: {req_id}")
        else:
            print(f"Go service returned error: {resp.status_code} {resp.text}")

    except Exception as e:
        print(f"Error processing/callback for ID {req_id}: {e}")


@api_view(['POST'])
def perform_calculation(request):
    """
    Входная точка. Принимает JSON от Go, запускает поток и возвращает 200.
    URL: /api/analysis/
    """
    try:
        data = request.data
        # Проверяем наличие ID
        if "id" not in data:
             return Response({"error": "No ID provided"}, status=status.HTTP_400_BAD_REQUEST)

        # Запускаем задачу в фоновом потоке
        executor.submit(calculate_logic, data)

        return Response({"message": "Calculation started"}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)