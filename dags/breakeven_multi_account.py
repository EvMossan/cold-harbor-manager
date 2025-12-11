"""
Dynamic_Breakeven_Manager.py
============================

Динамическая генерация DAG-ов для всех аккаунтов, указанных в
coldharbour_manager.core.destinations.DESTINATIONS.

Логика:
1. Импортирует список DESTINATIONS.
2. Для каждого аккаунта создает отдельный DAG с ID: "Risk_Manager_{slug}".
3. Прокидывает специфичные ключи (API, Secret, URL) и настройки таблиц.
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Импортируем ваши конфигурации и хелперы
from coldharbour_manager.core.destinations import DESTINATIONS, slug_for
from coldharbour_manager.services.risks_manager import BreakevenOrderManager

# Настройки по умолчанию для всех дагов
default_args = {
    "owner": "JJ",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Расписание: каждая минута торговой сессии (примерно)
SCHEDULE_CRON = "*/1 9-16 * * 1-5"

def create_dag(destination: dict):
    """
    Фабричная функция, создающая DAG для конкретного destination.
    """
    
    # 1. Генерация уникального ID (slug)
    # slug_for делает имена безопасными: "Cold Harbour v1.0" -> "cold_harbour_v1_0"
    account_slug = slug_for(destination)
    dag_id = f"Risk_Manager_{account_slug}"
    
    # Получаем имя аккаунта для логов/UI
    account_name = destination.get("name", account_slug)

    async def _run_async_task():
        """Асинхронная логика задачи"""
        # Настройка логгера под конкретный DAG
        logger = logging.getLogger(f"BE_Manager_{account_slug}")
        logger.setLevel(logging.INFO)
        logger.info(f"Starting Risk Manager cycle for: {account_name} ({account_slug})")

        # 2. Сборка конфигурации для конкретного аккаунта
        # Берем данные прямо из словаря destination, где они уже загружены через os.getenv
        
        # Определяем таблицы. 
        # Breakouts и Bars - общие. Logs - индивидуальная по слагу.
        log_table = f"log_breakeven_{account_slug}"
        
        config = {
            # --- Alpaca Credentials ---
            # destinations.py уже сделал os.getenv, поэтому берем значения напрямую
            "API_KEY":         destination.get("key_id"),
            "SECRET_KEY":      destination.get("secret_key"),
            "ALPACA_BASE_URL": destination.get("base_url", "https://paper-api.alpaca.markets"),

            # --- Connections ---
            "CONN_STRING_POSTGRESQL": os.getenv(
                "POSTGRESQL_LIVE_LOCAL_CONN_STRING",
                "postgresql://user:pass@localhost:5432/live"
            ),
            "CONN_STRING_TIMESCALE": os.getenv(
                "TIMESCALE_LIVE_LOCAL_CONN_STRING",
                "postgresql://user:pass@localhost:5432/timescale"
            ),

            # --- Tables ---
            "TABLE_BARS_1MIN": "alpaca_bars_1min",      # Общая
            "TABLE_BREAKOUTS": "alpaca_breakouts",      # Общая
            "TABLE_BE_EVENTS": log_table,               # Индивидуальная (log_stop_manager_live и т.д.)
            
            "ACCOUNT_SLUG": account_slug,

            # --- Tuning ---
            "BE_TRIGGER_R": 2,
            "TICK_DEFAULT": 0.01,
            "MIN_STOP_GAP": 0.01,
            "PRICE_DECIMALS": Decimal("0.01"),
            
            # Можно переопределить Risk Factor или Dry Run из конфига destination, если нужно
            "DRY_RUN": destination.get("dry_run", False),
            "MAX_WORKERS": 10,
        }

        # Валидация ключей (на случай если env vars не прогрузились)
        if not config["API_KEY"] or not config["SECRET_KEY"]:
            logger.error(f"Missing credentials for {account_name}. Skipping.")
            return

        # 3. Запуск менеджера
        om = BreakevenOrderManager(config)
        try:
            await om.run()
        finally:
            await om.close()

    def task_wrapper():
        """Синхронная обертка для Airflow PythonOperator"""
        asyncio.run(_run_async_task())

    # 4. Создание объекта DAG
    dag = DAG(
        dag_id=dag_id,
        description=f"Break-Even manager for {account_name}",
        default_args=default_args,
        start_date=datetime(2025, 7, 14),
        schedule=SCHEDULE_CRON,
        catchup=False,
        tags=["Risk Manager", "Multi-Account", account_slug],
    )

    with dag:
        PythonOperator(
            task_id=f"run_be_cycle_{account_slug}",
            python_callable=task_wrapper,
        )

    return dag

# -----------------------------------------------------------------------
# ГЛАВНЫЙ ЦИКЛ РЕГИСТРАЦИИ (Dynamic DAG Generation)
# -----------------------------------------------------------------------

# Проходим по списку из destinations.py
for dest in DESTINATIONS:
    # Пропускаем, если trading explicitly disabled (опционально)
    if not dest.get("allow_trading", True):
        continue

    # Создаем DAG
    generated_dag = create_dag(dest)
    
    # ВАЖНО: Регистрируем DAG в глобальной области видимости.
    # Airflow ищет объекты DAG именно в globals().
    globals()[generated_dag.dag_id] = generated_dag