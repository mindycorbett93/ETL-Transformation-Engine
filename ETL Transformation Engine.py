import pandas as pd
import json
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict

# --- Master Schema Definition (Standardized Map) ---
class MasterEncounterSchema(BaseModel):
    patient_id: str = Field(..., alias="PatientID")
    billing_npi: str = Field(..., regex=r"^\d{10}$")
    dos: str = Field(..., alias="DateOfService")
    cpt_codes: List[str]
    icd_10: List[str]
    total_charges: float

# --- Interoperability recursive mapper ---
class ETLTransformationEngine:
    def __init__(self, mapping_config: Dict):
        self.master_map = mapping_config # Map standard keys to list of legacy aliases

    def map_legacy_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recursive fuzzy matching for legacy EHR headers (S_R39, S_R62)."""
        standardized_data = {}
        for standard_key, aliases in self.master_map.items():
            # Find which legacy header exists in this specific EHR export
            found_alias = next((a for a in aliases if a in df.columns), None)
            if found_alias:
                standardized_data[standard_key] = df[found_alias]
            else:
                standardized_data[standard_key] = None # Flag for manual mapping
        return pd.DataFrame(standardized_data)

    def generate_agnostic_payload(self, df: pd.DataFrame, format="json"):
        """Generates payloads ready for any enterprise billing platform."""
        if format == "json":
            return df.to_json(orient="records")
        elif format == "xml":
            return df.to_xml()
        return df.to_csv(index=False)
