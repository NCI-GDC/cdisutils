import imp, os, logging, json
from pprint import pprint
import unittest

logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s %(name)-6s %(levelname)-4s %(message)s' )

# Import utils
currentDir = os.path.dirname(os.path.realpath(__file__))
convertDir = os.path.dirname(currentDir)
utilPath   = os.path.join(convertDir, 'xml2json.py')
util       = imp.load_source('xml2json', utilPath)

xml_sample = "sample.xml"
json_sample = "sample_flat.json"

class ConversionTest(unittest.TestCase):


    def setUp(self):
        
        conv = util.xml2json()
        conv.loadFromFile(xml_sample)
        docs = conv.toJSON(flatten=True)

        self.test = docs

        with open(json_sample, 'r') as f:
            self.sample = json.load(f)
    
    def test_contains_sample_docs(self):
        
        sample_json = self.sample
        test_json = self.test

        for sample_doc in sample_json:
            sample_type = sample_doc['_type']
            test_docs = [doc for doc in test_json if doc['_type'] == sample_type]
            self.assertEqual(len(test_docs), 1, msg = 'There were multiple test docs with type [{type}]'.format(type = sample_type))
            test_doc = test_docs[0]
            for sample_key, sample_value in sample_doc.iteritems():
                self.assertTrue(sample_key in test_doc, msg = 'Key error: {key} not found in test doc]'.format(key = sample_key))
                self.assertEqual(sample_value, test_doc[sample_key], msg = 'Sample[{key}] value [{sample}] not equal to [{test}]'.format(
                        key = sample_key, sample = sample_value, test = test_doc[sample_key]))
    
    def test_doesnt_contain_extra_docs(self):    
        sample_json = self.test
        test_json = self.sample

        for sample_doc in sample_json:
            sample_type = sample_doc['_type']
            test_docs = [doc for doc in test_json if doc['_type'] == sample_type]
            self.assertEqual(len(test_docs), 1, msg = 'Doc with type [{type}] was found but shouldn\'t exist'.format(type = sample_type))
            test_doc = test_docs[0]
            for sample_key, sample_value in sample_doc.iteritems():
                self.assertTrue(sample_key in test_doc, msg = 'Key error: {key} should not exist in doc with type [{type}]'.format(key = sample_key, type = sample_type))
                self.assertEqual(sample_value, test_doc[sample_key], msg = 'Sample[{key}] value [{sample}] not equal to [{test}]'.format(
                        key = sample_key, sample = sample_value, test = test_doc[sample_key]))
    

if __name__ == '__main__':
    unittest.main()
