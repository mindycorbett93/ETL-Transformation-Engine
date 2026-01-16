import json
import logging

class RCMDataMapper:
    def __init__(self, mapping_config):
        # The Mapping Dictionary: Maps Legacy Keys to Standard RCM Keys
        self.field_map = mapping_config
        self.standard_schema = ["patient_id", "rendering_npi", "cpt_code", "icd10_code", "charge_amount"]

    def validate_record(self, record):
        """
        Ensures mandatory data exists and meets basic RCM standards.
        """
        # Validation: Check for nulls in mandatory fields
        for field in self.standard_schema:
            if not record.get(field):
                return False, f"Missing mandatory field: {field}"
        
        # Validation: Basic NPI Length check
        if len(str(record['rendering_npi'])) != 10:
            return False, "Invalid NPI length."
            
        return True, "Valid"

    def transform_legacy_data(self, legacy_batch):
        """
        The ETL Core: Extract, Transform, and Validate.
        """
        standardized_batch = []
        logs = []

        for row in legacy_batch:
            # 1. Transform: Map legacy fields to Standard Schema
            transformed_row = {
                std_key: row.get(legacy_key) 
                for std_key, legacy_key in self.field_map.items()
            }

            # 2. Validate: Run transformed row through RCM quality gate
            is_valid, msg = self.validate_record(transformed_row)
            
            if is_valid:
                standardized_batch.append(transformed_row)
            else:
                logs.append({"record": row.get('Legacy_ID', 'Unknown'), "error": msg})

        return standardized_batch, logs

# --- Example Implementation ---
# Mapping Dictionary based on an eClinicalWorks to Experity migration logic
ecw_to_standard_map = {
    "patient_id": "Pt_Account_Num",
    "rendering_npi": "Provider_NPI",
    "cpt_code": "Procedure_Code",
    "icd10_code": "Diagnosis_Primary",
    "charge_amount": "Line_Total"
}

# Mock Legacy Data Extract
legacy_data = [
    {"Pt_Account_Num": "99887", "Provider_NPI": "1234567890", "Procedure_Code": "99214", "Diagnosis_Primary": "M17.11", "Line_Total": 150.00},
    {"Pt_Account_Num": "99888", "Provider_NPI": "000", "Procedure_Code": "99213", "Diagnosis_Primary": "J01.90", "Line_Total": 110.00} # Fails validation
]

mapper = RCMDataMapper(ecw_to_standard_map)
clean_data, error_log = mapper.transform_legacy_data(legacy_data)

print(f"Successfully Standardized: {len(clean_data)} records.")
print(f"Validation Errors Found: {len(error_log)}")