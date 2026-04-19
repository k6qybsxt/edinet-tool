from __future__ import annotations

import shutil
import unittest
from pathlib import Path

from edinet_pipeline.services.linkbase_analyzer import analyze_linkbase_structure


LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
TERSE_ROLE = "http://www.xbrl.org/2003/role/terseLabel"
TOTAL_ROLE = "http://disclosure.edinet-fsa.go.jp/jppfs/sec/role/totalLabel"
STATEMENT_ROLE = "http://example.com/role/StatementOfIncome"
DIMENSION_ROLE = "http://example.com/role/RevenueTable"


LAB_XML = f"""<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_Revenue" xlink:label="Revenue" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_RevenueTotal" xlink:label="RevenueTotal" />
    <link:label xlink:type="resource" xlink:label="label_revenue" xlink:role="{LABEL_ROLE}" xml:lang="en">Revenue</link:label>
    <link:label xlink:type="resource" xlink:label="terse_revenue" xlink:role="{TERSE_ROLE}" xml:lang="en">Rev.</link:label>
    <link:label xlink:type="resource" xlink:label="total_revenue" xlink:role="{TOTAL_ROLE}" xml:lang="en">Revenue total</link:label>
    <link:labelArc xlink:type="arc" xlink:from="Revenue" xlink:to="label_revenue" />
    <link:labelArc xlink:type="arc" xlink:from="Revenue" xlink:to="terse_revenue" />
    <link:labelArc xlink:type="arc" xlink:from="RevenueTotal" xlink:to="total_revenue" />
  </link:labelLink>
</link:linkbase>
"""


PRE_XML = f"""<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:presentationLink xlink:type="extended" xlink:role="{STATEMENT_ROLE}">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_RevenueTotal" xlink:label="RevenueTotal" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_Revenue" xlink:label="Revenue" />
    <link:presentationArc xlink:type="arc" xlink:from="RevenueTotal" xlink:to="Revenue" />
  </link:presentationLink>
</link:linkbase>
"""


CAL_XML = f"""<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:calculationLink xlink:type="extended" xlink:role="{STATEMENT_ROLE}">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_RevenueTotal" xlink:label="RevenueTotal" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_Revenue" xlink:label="Revenue" />
    <link:calculationArc xlink:type="arc" xlink:from="RevenueTotal" xlink:to="Revenue" weight="1" />
  </link:calculationLink>
</link:linkbase>
"""


DEF_XML = f"""<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:definitionLink xlink:type="extended" xlink:role="{DIMENSION_ROLE}">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpcrp_cor_RevenueTable" xlink:label="RevenueTable" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpcrp_cor_RevenueAxis" xlink:label="RevenueAxis" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpcrp_cor_TotalMember" xlink:label="TotalMember" />
    <link:definitionArc xlink:type="arc" xlink:arcrole="http://xbrl.org/int/dim/arcrole/all" xlink:from="RevenueTable" xlink:to="RevenueAxis" />
    <link:definitionArc xlink:type="arc" xlink:arcrole="http://xbrl.org/int/dim/arcrole/dimension-default" xlink:from="RevenueAxis" xlink:to="TotalMember" />
  </link:definitionLink>
</link:linkbase>
"""


XSD_XML = """<?xml version="1.0" encoding="utf-8"?>
<xsd:schema
  xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  xmlns:xbrli="http://www.xbrl.org/2003/instance"
  xmlns:jppfs_cor="http://example.com/jppfs"
  targetNamespace="http://example.com/jppfs">
  <xsd:element name="Revenue" id="jppfs_cor_Revenue" type="xbrli:monetaryItemType" substitutionGroup="xbrli:item" xbrli:periodType="duration" xbrli:balance="credit" nillable="true" abstract="false" />
  <xsd:element name="RevenueTotal" id="jppfs_cor_RevenueTotal" type="xbrli:monetaryItemType" substitutionGroup="xbrli:item" xbrli:periodType="duration" xbrli:balance="credit" nillable="true" abstract="false" />
</xsd:schema>
"""


class LinkbaseMetadataTest(unittest.TestCase):
    def test_analyze_linkbase_structure_returns_roles_definition_and_schema_metadata(self) -> None:
        tmp_dir = Path("tests") / "_tmp_linkbase_metadata"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            xbrl_path = tmp_dir / "sample.xbrl"
            xbrl_path.write_text("<xbrli:xbrl/>", encoding="utf-8")
            (tmp_dir / "sample_lab.xml").write_text(LAB_XML, encoding="utf-8")
            (tmp_dir / "sample_pre.xml").write_text(PRE_XML, encoding="utf-8")
            (tmp_dir / "sample_cal.xml").write_text(CAL_XML, encoding="utf-8")
            (tmp_dir / "sample_def.xml").write_text(DEF_XML, encoding="utf-8")
            (tmp_dir / "sample.xsd").write_text(XSD_XML, encoding="utf-8")

            structure = analyze_linkbase_structure(xbrl_path=str(xbrl_path))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        revenue = structure["Revenue"]
        self.assertEqual(revenue["labels_by_role"][LABEL_ROLE], "Revenue")
        self.assertEqual(revenue["labels_by_role"][TERSE_ROLE], "Rev.")
        self.assertIn(STATEMENT_ROLE, revenue["presentation_roles"])
        self.assertIn(STATEMENT_ROLE, revenue["calculation_roles"])
        self.assertEqual(revenue["schema"]["type"], "xbrli:monetaryItemType")
        self.assertEqual(revenue["schema"]["period_type"], "duration")
        self.assertEqual(revenue["schema"]["balance"], "credit")
        self.assertEqual(revenue["calculation_relationships"][0]["weight"], 1.0)

        revenue_table = structure["RevenueTable"]
        self.assertIn(DIMENSION_ROLE, revenue_table["definition_roles"])
        self.assertEqual(
            revenue_table["definition_relationships"][0]["arcrole"],
            "http://xbrl.org/int/dim/arcrole/all",
        )


if __name__ == "__main__":
    unittest.main()
