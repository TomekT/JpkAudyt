import logging
import os
from pathlib import Path
from app.services.etl import etl_service
from app.services.legacy_etl import legacy_etl_service

logger = logging.getLogger(__name__)

class JPKRouter:
    """
    Orchestrator responsible for detecting the version of JPK_KR files 
    and routing the import process to the appropriate ETL service.
    """

    # Namespace constants
    NS_LEGACY = "http://jpk.mf.gov.pl/wzor/2016/03/09/03091/"
    NS_MODERN = "http://jpk.mf.gov.pl/wzor/2024/09/04/09041/"

    def __init__(self):
        # The services are imported from their respective modules where they are already instantiated
        self.modern_service = etl_service
        self.legacy_service = legacy_etl_service

    def _detect_version(self, file_path: str) -> str:
        """
        Detects JPK_KR version by reading the beginning of the XML file and looking for namespace URIs.
        Reads only the first 4096 bytes to ensure low memory footprint.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Plik nie istnieje: {file_path}")

        try:
            with open(file_path, "rb") as f:
                # Read initial segment of the file to find the namespace declaration
                chunk = f.read(4096).decode("utf-8", errors="ignore")
                
                if self.NS_MODERN in chunk:
                    return "MODERN"
                elif self.NS_LEGACY in chunk:
                    return "LEGACY"
                else:
                    logger.error(f"Unknown namespace in file: {file_path}")
                    raise ValueError("Nie rozpoznano formatu pliku JPK_KR (nieobsługiwana przestrzeń nazw).")
        except Exception as e:
            logger.error(f"Error during version detection: {e}")
            raise

    def route_import(self, file_path: str) -> str:
        """
        Detects the file version and routes it to either ETLService (Modern) or LegacyETLService (Legacy).
        Returns the path to the resulting SQLite database.
        """
        logger.info(f"Starting routing for file: {file_path}")
        
        try:
            version = self._detect_version(file_path)
            
            if version == "MODERN":
                logger.info("Routing to ETLService based on modern namespace detection (JPK_KR_PD).")
                # Calls the modern ETL service (from etl.py)
                return self.modern_service.import_jpk(file_path)
            
            elif version == "LEGACY":
                logger.info("Routing to LegacyETLService based on legacy namespace detection (JPK_KR).")
                # Calls the legacy ETL service (from legacy_etl.py)
                return self.legacy_service.import_jpk(file_path)
            
            else:
                raise ValueError(f"Unsupported JPK version: {version}")

        except Exception as e:
            logger.error(f"Import process failed in JPKRouter: {e}")
            raise

# Instance for easy import
jpk_router = JPKRouter()
