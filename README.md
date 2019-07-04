# stock_data_lambda

a mini AWS Lambda function for integration with API proxy gateway

use packaged .jar file is located under /target/ folder to upload to Lambda management console.

[guide on creating a API endpoint to integrate with Lambda](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html)

API format:

api_url/stage/getstockdata?field=ff_sales,ff_net_income&ticker=aapl-us,7203-jp&period=a