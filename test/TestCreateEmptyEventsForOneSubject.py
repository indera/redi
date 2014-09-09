import unittest
import os
from lxml import etree
import redi

DEFAULT_DATA_DIRECTORY = os.getcwd()

class TestCreateEmptyEventsForOneSubject(unittest.TestCase):

    def setUp(self):
        self.xml1_test1 = """<?xml version='1.0' encoding='utf-8'?>
<redcapProject>
    <name>Project</name>
    <form>
        <name>cbc</name>
        <formDateField>cbc_lbdtc</formDateField>
        <formCompletedFieldName>cbc_complete</formCompletedFieldName>
        <formImportedFieldName>cbc_nximport</formImportedFieldName>
        <event>
            <name>1_arm_1</name>
        </event>
        </form>
    <form>
        <name>chemistry</name>
        <formDateField>chem_lbdtc</formDateField>
        <formCompletedFieldName>chemistry_complete</formCompletedFieldName>
        <formImportedFieldName>chem_nximport</formImportedFieldName>
        <event>
            <name>1_arm_1</name>
        </event>
    </form>
    <form>
        <name>inr</name>
        <formDateField>inr_lbdtc</formDateField>
        <formCompletedFieldName>inr_complete</formCompletedFieldName>
        <formImportedFieldName>inr_nximport</formImportedFieldName>
        <event>
            <name>1_arm_1</name>
        </event>
    </form>
</redcapProject>
        """
        self.data1_test1 = etree.ElementTree(etree.fromstring(self.xml1_test1))
        
        self.xml2_test1 = """<?xml version='1.0' encoding='utf-8'?>
<rediFieldMap>
    <clinicalComponent>
        <clinicalComponentId>918</clinicalComponentId>
        <clinicalComponentName>LYMPHOCYTES</clinicalComponentName>
        <redcapFormName>cbc</redcapFormName>
        <redcapFieldNameValue>lymce_lborres</redcapFieldNameValue>
        <redcapFieldNameValueDescriptiveText>Lymphocytes (%)</redcapFieldNameValueDescriptiveText>
        <redcapFieldNameUnits>lymce_lborresu</redcapFieldNameUnits>
        <redcapFieldNameUnitsDescriptiveText>Lymphocytes (%) units</redcapFieldNameUnitsDescriptiveText>
        <redcapStatusFieldName>lymce_lbstat</redcapStatusFieldName>
        <lbtest>lymce_lbtest</lbtest>
        <lbtestcd>lymce_lbtestcd</lbtestcd>
    </clinicalComponent>
    <clinicalComponent>
        <clinicalComponentId>1534435</clinicalComponentId>
        <clinicalComponentName>HEMOGLOBIN</clinicalComponentName>
        <redcapFormName>cbc</redcapFormName>
        <redcapFieldNameValue>hemo_lborres</redcapFieldNameValue>
        <redcapFieldNameValueDescriptiveText>Hemoglobin</redcapFieldNameValueDescriptiveText>
        <redcapFieldNameUnits>hemo_lborresu</redcapFieldNameUnits>
        <redcapFieldNameUnitsDescriptiveText>Hemoglobin units</redcapFieldNameUnitsDescriptiveText>
        <redcapStatusFieldName>hemo_lbstat</redcapStatusFieldName>
        <lbtest>hemo_lbtest</lbtest>
        <lbtestcd>hemo_lbtestcd</lbtestcd>
    </clinicalComponent>
    <clinicalComponent>
        <clinicalComponentId>971</clinicalComponentId>
        <clinicalComponentName>POTASSIUM</clinicalComponentName>
        <redcapFormName>chemistry</redcapFormName>
        <redcapFieldNameValue>k_lborres</redcapFieldNameValue>
        <redcapFieldNameValueDescriptiveText>Potassium</redcapFieldNameValueDescriptiveText>
        <redcapFieldNameUnits>k_lborresu</redcapFieldNameUnits>
        <redcapFieldNameUnitsDescriptiveText>Potassium units</redcapFieldNameUnitsDescriptiveText>
        <redcapStatusFieldName>k_lbstat</redcapStatusFieldName>
        <lbtest></lbtest>
        <lbtestcd></lbtestcd>
    </clinicalComponent>
    <clinicalComponent>
        <clinicalComponentId>1534098</clinicalComponentId>
        <clinicalComponentName>SODIUM</clinicalComponentName>
        <redcapFormName>chemistry</redcapFormName>
        <redcapFieldNameValue>sodium_lborres</redcapFieldNameValue>
        <redcapFieldNameValueDescriptiveText>Sodium</redcapFieldNameValueDescriptiveText>
        <redcapFieldNameUnits>sodium_lborresu</redcapFieldNameUnits>
        <redcapFieldNameUnitsDescriptiveText>Sodium units</redcapFieldNameUnitsDescriptiveText>
        <redcapStatusFieldName>sodium_lbstat</redcapStatusFieldName>
        <lbtest>Sodium</lbtest>
        <lbtestcd>SODIUM</lbtestcd>
    </clinicalComponent>
</rediFieldMap>
        """
        self.data2_test1 = etree.ElementTree(etree.fromstring(self.xml2_test1))
        self.output_test1 = """
<all_form_events>
    <form form_name="cbc">
        <name>cbc</name>
        <event>
            <name>1_arm_1</name>
            <field><name>lymce_lborres</name><value/></field><field><name>lymce_lborresu</name><value/></field><field><name>hemo_lborresu</name><value/></field><field><name>cbc_lbdtc</name><value/></field><field><name>cbc_nximport</name><value/></field><field><name>lymce_lbstat</name><value/></field><field><name>cbc_complete</name><value/></field><field><name>hemo_lbstat</name><value/></field><field><name>hemo_lborres</name><value/></field></event>
    </form>
    <form form_name="chemistry">
        <name>chemistry</name>
        <event>
            <name>1_arm_1</name>
            <field><name>k_lborres</name><value/></field><field><name>chem_lbdtc</name><value/></field><field><name>sodium_lborresu</name><value/></field><field><name>k_lbstat</name><value/></field><field><name>sodium_lbstat</name><value/></field><field><name>chem_nximport</name><value/></field><field><name>chemistry_complete</name><value/></field><field><name>k_lborresu</name><value/></field><field><name>sodium_lborres</name><value/></field>
        </event>
    </form>
    <form form_name="inr">
        <name>inr</name>
        <event>
            <name>1_arm_1</name>
            <field><name>inr_lbdtc</name><value/></field><field><name>inr_complete</name><value/></field><field><name>inr_nximport</name><value/></field>
        </event>
    </form>
</all_form_events>"""

        self.expect_test1 = etree.tostring(etree.fromstring(self.output_test1))
        
        self.xml1_test2 = """<?xml version='1.0' encoding='utf-8'?>
<redcapProject>
    <name>Project</name>
    <form>
        <name>cbc</name>
        <event>
           <name>1_arm_1</name>
        </event>
    </form>
    <form>
        <name>chemistry</name>
        <event>
            <name>1_arm_1</name>
        </event>
    </form>
    <form>
        <name>inr</name>
        <event>
            <name>1_arm_1</name>
        </event>
   </form>
</redcapProject>"""

        self.data1_test2 = etree.ElementTree(etree.fromstring(self.xml1_test2))
        
        self.xml2_test2 = """<?xml version='1.0' encoding='utf-8'?>
<rediFieldMap>
    <clinicalComponent>
        <clinicalComponentId>918</clinicalComponentId>
        <clinicalComponentName>LYMPHOCYTES</clinicalComponentName>
        <redcapFormName>cbc</redcapFormName>
        <lbtest>lymce_lbtest</lbtest>
        <lbtestcd>lymce_lbtestcd</lbtestcd>
    </clinicalComponent>
    <clinicalComponent>
        <clinicalComponentId>1534435</clinicalComponentId>
        <clinicalComponentName>HEMOGLOBIN</clinicalComponentName>
        <redcapFormName>cbc</redcapFormName>
        <lbtest>hemo_lbtest</lbtest>
        <lbtestcd>hemo_lbtestcd</lbtestcd>
    </clinicalComponent>
    <clinicalComponent>
        <clinicalComponentId>971</clinicalComponentId>
        <clinicalComponentName>POTASSIUM</clinicalComponentName>
        <redcapFormName>chemistry</redcapFormName>
        <lbtest></lbtest>
        <lbtestcd></lbtestcd>
    </clinicalComponent>

    <clinicalComponent>
        <clinicalComponentId>1534098</clinicalComponentId>
        <clinicalComponentName>SODIUM</clinicalComponentName>
        <redcapFormName>chemistry</redcapFormName>
        <lbtest>Sodium</lbtest>
        <lbtestcd>SODIUM</lbtestcd>
    </clinicalComponent>
</rediFieldMap>"""

        self.data2_test2 = etree.ElementTree(etree.fromstring(self.xml2_test2))

        self.output_test2 = """
<all_form_events>
    <form>
        <name>cbc</name>
        <event>
            <name>1_arm_1</name>
            <field><name>lymce_lborres</name><value/></field><field><name>lymce_lborresu</name><value/></field><field><name>hemo_lborresu</name><value/></field><field><name>cbc_lbdtc</name><value/></field><field><name>cbc_nximport</name><value/></field><field><name>lymce_lbstat</name><value/></field><field><name>cbc_complete</name><value/></field><field><name>hemo_lbstat</name><value/></field><field><name>hemo_lborres</name><value/></field>i
        </event>
    </form>
    <form>
        <name>chemistry</name>
        <event>
            <name>1_arm_1</name>
            <field><name>k_lborres</name><value/></field><field><name>chem_lbdtc</name><value/></field><field><name>sodium_lborresu</name><value/></field><field><name>k_lbstat</name><value/></field><field><name>sodium_lbstat</name><value/></field><field><name>chem_nximport</name><value/></field><field><name>chemistry_complete</name><value/></field><field><name>k_lborresu</name><value/></field><field><name>sodium_lborres</name><value/></field>
        </event>
    </form>
<form>
    <name>inr</name>
    <event>
        <name>1_arm_1</name>
        <field><name>inr_lbdtc</name><value/></field><field><name>inr_complete</name><value/></field><field><name>inr_nximport</name><value/></field>
    </event>
   </form>
</all_form_events>"""
        self.expect_test2 = etree.tostring(etree.fromstring(self.output_test2))
        return()

    def test_create_empty_events_for_one_subject_all_fields_input(self):
        redi.configure_logging(DEFAULT_DATA_DIRECTORY)
        self.result = etree.tostring(redi.create_empty_events_for_one_subject(self.data1_test1,self.data2_test1))
        self.result = ''.join(self.result.split())
        self.expect_test1 = ''.join(self.expect_test1.split())
        self.assertEqual(self.expect_test1, self.result)

    def test_create_empty_events_for_one_subject_no_fields_input(self):
        redi.configure_logging(DEFAULT_DATA_DIRECTORY)
        self.assertRaises(Exception,redi.create_empty_events_for_one_subject,self.data1_test2,self.data2_test2)

    def tearDown(self):
        return()

if __name__ == '__main__':
    unittest.main()
