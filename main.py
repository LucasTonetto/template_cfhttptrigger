from constructs import Construct
from cdktf import App, TerraformStack
from lib.CloudFunctionHttpTrigger import CloudFunctionHttpTrigger

# pipenv run
# export GOOGLE_APPLICATION_CREDENTIALS='<<ABSOLUTE_PATH_FOR_ACCOUNT_KEY.JSON>>'
# pipenv install cdktf-cdktf-provider-null

# bq rm -j dp6-estudos:US.job_insert_test_data_for_tests-enviroments-aut

class MyStack(TerraformStack):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        CloudFunctionHttpTrigger(
            self,
            '#STACK_NAME',
            'PROJECT',
            'REGION',
            'CF_NAME',
            'cf_code',
            'BUCKET_FOR_CF',
            prod_environment=False,
            dataset_prod='DATASET_PROD',
            dataset_dev='DATASET_DEV',
            table_name='TABLE_NAME'
        )

app = App()
MyStack(app, "STACK")

app.synth()
