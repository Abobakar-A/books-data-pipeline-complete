# book_pipeline_dag.py

# استيراد المكتبات الضرورية
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# استيراد الدوال من الملف الآخر
# هام: يجب أن يكون هذا الملف (book_pipeline_tasks.py) في نفس المجلد.
from scrape_books import create_snowflake_table, scrape_and_load_books

# --- تعريف الـ DAG في Airflow ---
with DAG(
    dag_id='book_scraper_snowflake_dbt_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['example', 'data-pipeline', 'books', 'dbt'],
) as dag:
    # 1. مهمة إنشاء الجدول
    create_table_task = PythonOperator(
        task_id='create_books_table',
        python_callable=create_snowflake_table,
    )
    
    # 2. مهمة استخراج وتحميل البيانات
    scrape_and_load_task = PythonOperator(
        task_id='scrape_and_load_books',
        python_callable=scrape_and_load_books,
    )
    
    # 3. مهمة تشخيصية: لعرض محتويات المجلد الرئيسي
    # هذا سيساعدك على تأكيد مكان مجلد مشروع dbt
    check_dbt_project_dir = BashOperator(
        task_id="check_dbt_project_dir",
        bash_command="ls -la /usr/local/airflow/",
    )

    # 4. مهمة تشغيل مشروع dbt لتحويل البيانات
    # هذا هو السطر الذي يحل المشكلة.
    DBT_PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'my_new_dbt_project')
    dbt_run_models = BashOperator(
        task_id="dbt_run_models",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR}",
        cwd=DBT_PROJECT_DIR
    )

    # تحديد ترتيب تنفيذ المهام
    create_table_task >> scrape_and_load_task >> check_dbt_project_dir >> dbt_run_models
