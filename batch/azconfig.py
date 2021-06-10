# Global constant variables (Azure Storage account/Batch details)

# import "config.py" in "python_quickstart_client.py "

_BATCH_ACCOUNT_NAME = 'cic'  # Your batch account name
_BATCH_ACCOUNT_KEY = 'YOURBATCHKEY'  # Your batch account key
_BATCH_ACCOUNT_URL = 'https://cic.eastus.batch.azure.com'  # Your batch account URL
_STORAGE_ACCOUNT_NAME = 'cbpbigshare'  # Your storage account name
_STORAGE_ACCOUNT_KEY = 'YOURACCOUNTKEY'  # Your storage account key
_POOL_ID = 'TestPoolID1'  # Your Pool ID
_POOL_NODE_COUNT = 2  # Pool node count
_POOL_VM_SIZE = 'Standard_D2_v3'  # VM Type/Size MUST BE D2_v{}

_JOB_ID = 'TestJob1'  # Job ID
_STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file.
_CFLIST = ['augu_51015',
            'balt_24005',
            'berk_54003',
            'brad_42015',
            'chen_36017',
            'clea_42033',
            'cumb_42041',
            'glou_51073',
            'hard_54031',
            'lanc_42071',
            'loud_51107',
            'quee_24035',
            'suss_10005',
            'wico_24045']

_landuse_version = 1.4


"""
NOTES:
added CFLIST
job id could be individual scripts?
should this batching run 1 county through all steps or 


"""