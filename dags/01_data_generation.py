"""
DAG 1: توليد البيانات الوهمية وإرسالها إلى كافكا
الهدف: محاكاة المعاملات المصرفية بشكل مستمر
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import random

# ============================================
# دالة توليد المعاملات
# ============================================

def generate_transactions(**context):
    """
    توليد معاملات مصرفية وهمية وإرسالها إلى كافكا
    يتم استيراد KafkaProducer هنا لتجنب مشاكل الإقلاع في خادم الويب
    """
    from kafka import KafkaProducer

    # قراءة الإعدادات من متغيرات البيئة
    bootstrap_servers = Variable.get(
        "KAFKA_BOOTSTRAP_SERVERS",
        default_var="kafka1:29092,kafka2:29093,kafka3:29094"
    )
    topic = Variable.get(
        "KAFKA_TOPIC_RAW",
        default_var="raw_transactions"
    )
    
    print(f"📤 الاتصال بكافكا: {bootstrap_servers}")
    print(f"📤 الإرسال إلى الموضوع: {topic}")
    
    try:
        # إنشاء منتج كافكا
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=10000,
            request_timeout_ms=10000
        )
    except Exception as e:
        print(f"❌ فشل إنشاء منتج كافكا: {e}")
        raise
    
    num_transactions = 100
    fraud_count = 0
    
    # توليد المعاملات
    for i in range(num_transactions):
        is_fraud = (i % 100 == 0)
        
        if not is_fraud:
            transaction = {
                "transaction_id": random.randint(100000, 999999),
                "user_id": random.randint(1, 100),
                "amount": round(random.uniform(5, 500), 2),
                "currency": random.choice(['USD', 'EUR', 'GBP']),
                "timestamp": datetime.now().isoformat(),
                "country": random.choice(["US", "GB", "FR", "DE", "AE", "SA"]),
                "merchant": random.choice(["Amazon", "Apple", "Google", "Nike", "Shell"]),
                "device_id": f"DEV_{random.randint(1000, 9999)}",
                "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                "is_fraud": 0
            }
        else:
            fraud_count += 1
            if random.random() < 0.6:
                transaction = {
                    "transaction_id": random.randint(100000, 999999),
                    "user_id": random.randint(1, 100),
                    "amount": round(random.uniform(5000, 20000), 2),
                    "currency": "USD",
                    "timestamp": datetime.now().isoformat(),
                    "country": random.choice(["CN", "RU", "KP", "IR"]),
                    "merchant": "Unknown Merchant",
                    "device_id": f"DEV_{random.randint(1000, 9999)}",
                    "ip_address": f"10.0.{random.randint(1,255)}.{random.randint(1,255)}",
                    "is_fraud": 1
                }
            else:
                transaction = {
                    "transaction_id": random.randint(100000, 999999),
                    "user_id": random.randint(1, 100),
                    "amount": round(random.uniform(10, 99), 2),
                    "currency": "USD",
                    "timestamp": datetime.now().isoformat(),
                    "country": "US",
                    "merchant": "Small Store",
                    "device_id": f"DEV_{random.randint(1000, 9999)}",
                    "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                    "is_fraud": 1
                }
        
        try:
            producer.send(topic, value=transaction)
        except Exception as e:
            print(f"❌ فشل إرسال المعاملة {transaction['transaction_id']}: {e}")
    
    producer.flush()
    producer.close()
    
    # تخزين النتائج في XCom
    context['ti'].xcom_push(key='fraud_count', value=fraud_count)
    context['ti'].xcom_push(key='total_count', value=num_transactions)
    
    print(f"✅ تم توليد {num_transactions} معاملة ({fraud_count} احتيال)")
    return f"Generated {num_transactions} transactions"

# ============================================
# تعريف DAG
# ============================================

default_args = {
    'owner': 'fraud_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),  # تاريخ ثابت في الماضي
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    '01_data_generation',
    default_args=default_args,
    description='توليد بيانات وهمية للمعاملات المصرفية وإرسالها إلى كافكا',
    schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['generation', 'kafka', 'fraud']
)

# ============================================
# تعريف المهام
# ============================================

start = EmptyOperator(task_id='start', dag=dag)

generate_task = PythonOperator(
    task_id='generate_fraud_data',
    python_callable=generate_transactions,
    dag=dag,
    retries=3
)

end = EmptyOperator(task_id='end', dag=dag)

start >> generate_task >> end