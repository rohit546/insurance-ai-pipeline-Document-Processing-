"""
Merge certificate ground-truth with policy validation outputs for UI consumption.

Inputs:
- cert_json: path to certificate extraction (e.g., *_pl_extracted_real.json)
- core_validation_json: path to llm_pl_pol.py output (e.g., *_validation.json)
- coverage_validation_json: path to llm_pl_pol_cov.py output (e.g., *_building_validation.json)

Output (stdout): consolidated JSON with:
{
  "certificate": {...},
  "core_validations": {...},
  "coverage_validations": {...},
  "summary": {...}
}
"""
import json
import sys
from pathlib import Path
from typing import Dict, Any


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def merge(cert: Dict[str, Any], core: Dict[str, Any], cov: Dict[str, Any]) -> Dict[str, Any]:
    core_val = core.get("validation_results", core)

    cov_summary = (cov.get("summary") or {}) if isinstance(cov, dict) else {}
    core_summary = (core.get("summary") or {}) if isinstance(core, dict) else {}

    summary = {
        "core": core_summary,
        "coverage": cov_summary,
    }

    return {
        "certificate": cert,
        "core_validations": core_val,
        "coverage_validations": {
            "building_validations": cov.get("building_validations", []),
            "bpp_validations": cov.get("bpp_validations", []),
            "business_income_validations": cov.get("business_income_validations", []),
            "money_securities_validations": cov.get("money_securities_validations", []),
            "equipment_breakdown_validations": cov.get("equipment_breakdown_validations", []),
            "outdoor_signs_validations": cov.get("outdoor_signs_validations", []),
            "employee_dishonesty_validations": cov.get("employee_dishonesty_validations", []),
            "pumps_canopy_validations": cov.get("pumps_canopy_validations", []),
            "theft_validations": cov.get("theft_validations", []),
            "wind_hail_validations": cov.get("wind_hail_validations", []),
        },
        "summary": summary,
    }


def main():
    if len(sys.argv) < 4:
        print("Usage: python merge_policy_results.py <cert_json> <core_validation_json> <coverage_validation_json>")
        sys.exit(1)

    cert_path = Path(sys.argv[1])
    core_path = Path(sys.argv[2])
    cov_path = Path(sys.argv[3])

    cert = load_json(cert_path)
    core = load_json(core_path)
    cov = load_json(cov_path)

    merged = merge(cert, core, cov)
    print(json.dumps(merged, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

