from app.services.database import db_service
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_database():
    try:
        conn = db_service.get_connection()
        cursor = conn.cursor()
        
        logger.info("Starting database cleaning: trimming account identifiers...")
        
        # 1. Clean ZOiS table
        logger.info("Cleaning ZOiS table (S_1, S_3)...")
        cursor.execute("UPDATE ZOiS SET S_1 = TRIM(S_1), S_3 = TRIM(S_3)")
        
        # 2. Clean Zapisy table
        logger.info("Cleaning Zapisy table (Z_3)...")
        cursor.execute("UPDATE Zapisy SET Z_3 = TRIM(Z_3)")
        
        conn.commit()
        logger.info("Database cleaning completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Error cleaning database: {e}")
        return False

if __name__ == "__main__":
    # Ensure a database is connected if possible
    from app.core.config import config_manager
    last_db = config_manager.get_last_db()
    if last_db:
        db_service.connect(last_db)
        clean_database()
    else:
        print("No active database found to clean.")
