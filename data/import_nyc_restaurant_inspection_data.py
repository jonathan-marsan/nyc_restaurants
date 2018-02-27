"""
Dataset Import Job
DOHMH New York City Restaurant Inspection Results
"""

from sodapy import Socrata

from modules.utilities import get_count, list_to_csv


def write_nyc_inspection_to_csv(token, output_csv,
                                source='data.cityofnewyork.us', data_id='9w7m-hzhe'):
    """
    Import DOHMH New York City Restaurant Inspection Results
    """
    client = Socrata(source, token)
    count = get_count(client=client, dataset=data_id)
    nyc_data = client.get(data_id, content_type="csv", limit=count)
    list_to_csv(items=nyc_data, path=output_csv)
    print("""
          Imported DOHMH NYC Restaurant Inspection Data
          Output: {0}
          """.format(output_csv))
