from __future__ import annotations

import unittest
from pathlib import Path
import shutil
from zipfile import ZipFile

from edinet_pipeline.services.linkbase_analyzer import analyze_linkbase_structure


LAB_XML = """<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_SellingGeneralAndAdministrativeExpenses" xlink:label="SellingGeneralAndAdministrativeExpenses" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_GeneralAndAdministrativeExpenses" xlink:label="GeneralAndAdministrativeExpenses" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpigp_cor_ExpenseIFRS" xlink:label="ExpenseIFRS" />
    <link:label xlink:type="resource" xlink:label="label_sga" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="ja">販売費及び一般管理費</link:label>
    <link:label xlink:type="resource" xlink:label="label_ga" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="ja">一般管理費</link:label>
    <link:label xlink:type="resource" xlink:label="label_expense" xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="ja">費用合計</link:label>
    <link:labelArc xlink:type="arc" xlink:from="SellingGeneralAndAdministrativeExpenses" xlink:to="label_sga" />
    <link:labelArc xlink:type="arc" xlink:from="GeneralAndAdministrativeExpenses" xlink:to="label_ga" />
    <link:labelArc xlink:type="arc" xlink:from="ExpenseIFRS" xlink:to="label_expense" />
  </link:labelLink>
</link:linkbase>
"""


PRE_XML = """<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:presentationLink xlink:type="extended" xlink:role="http://example.com/role/StatementOfIncome">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpigp_cor_ExpenseIFRS" xlink:label="ExpenseIFRS" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_SellingGeneralAndAdministrativeExpenses" xlink:label="SellingGeneralAndAdministrativeExpenses" />
    <link:presentationArc xlink:type="arc" xlink:from="ExpenseIFRS" xlink:to="SellingGeneralAndAdministrativeExpenses" />
  </link:presentationLink>
</link:linkbase>
"""


CAL_XML = """<?xml version="1.0" encoding="utf-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase" xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:calculationLink xlink:type="extended" xlink:role="http://example.com/role/StatementOfIncome">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jpigp_cor_ExpenseIFRS" xlink:label="ExpenseIFRS" />
    <link:loc xlink:type="locator" xlink:href="sample.xsd#jppfs_cor_SellingGeneralAndAdministrativeExpenses" xlink:label="SellingGeneralAndAdministrativeExpenses" />
    <link:calculationArc xlink:type="arc" xlink:from="ExpenseIFRS" xlink:to="SellingGeneralAndAdministrativeExpenses" weight="1" />
  </link:calculationLink>
</link:linkbase>
"""


class LinkbaseAnalyzerTest(unittest.TestCase):
    def test_analyze_linkbase_structure_reads_labels_and_parent_relationships(self) -> None:
        tmp_dir = Path("tests") / "_tmp_linkbase_analyzer"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            xbrl_path = tmp_dir / "sample.xbrl"
            xbrl_path.write_text("<xbrli:xbrl/>", encoding="utf-8")
            (tmp_dir / "sample_lab.xml").write_text(LAB_XML, encoding="utf-8")
            (tmp_dir / "sample_pre.xml").write_text(PRE_XML, encoding="utf-8")
            (tmp_dir / "sample_cal.xml").write_text(CAL_XML, encoding="utf-8")

            structure = analyze_linkbase_structure(xbrl_path=str(xbrl_path))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        self.assertEqual(structure["ExpenseIFRS"]["label"], "費用合計")
        self.assertTrue(structure["ExpenseIFRS"]["is_total"])
        self.assertEqual(
            structure["SellingGeneralAndAdministrativeExpenses"]["presentation_parent_labels"],
            ["費用合計"],
        )

    def test_analyze_linkbase_structure_reads_public_doc_companions_from_zip(self) -> None:
        tmp_dir = Path("tests") / "_tmp_linkbase_zip"
        shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        zip_path = tmp_dir / "sample.zip"
        try:
            with ZipFile(zip_path, "w") as zf:
                zf.writestr(
                    "S100AAAA/XBRL/PublicDoc/sample.xbrl",
                    "<xbrli:xbrl/>",
                )
                zf.writestr("S100AAAA/XBRL/PublicDoc/sample_lab.xml", LAB_XML)
                zf.writestr("S100AAAA/XBRL/PublicDoc/sample_pre.xml", PRE_XML)
                zf.writestr("S100AAAA/XBRL/PublicDoc/sample_cal.xml", CAL_XML)

            structure = analyze_linkbase_structure(
                xbrl_path="sample.xbrl",
                zip_path=str(zip_path),
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        self.assertEqual(structure["ExpenseIFRS"]["label"], "費用合計")
        self.assertEqual(
            structure["SellingGeneralAndAdministrativeExpenses"]["presentation_parent_labels"],
            ["費用合計"],
        )


if __name__ == "__main__":
    unittest.main()
