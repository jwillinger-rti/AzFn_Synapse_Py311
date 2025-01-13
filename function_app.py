import azure.functions as func
import logging
from cme.src import cme_download_http_response as cme
from eia.src import eia_download_http_response as eia

# func host start.
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# CME:
@app.route(route="cme_dowload_http_response")
def cme_dowload_http_response(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Python HTTP trigger function processed a request.")
    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("name")
    
    # Nest custom function here:
    b_success = cme.cme_download_http_reponse()
    print(f"Status of successful cme_download_http_reponse: {b_success}")
    if b_success:
        return func.HttpResponse(f"{name}: This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. No body received.",
             status_code=200
        )

# EIA:
@app.route(route="eia_dowload_http_response")
def eia_dowload_http_response(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Python HTTP trigger function processed a request.")
    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("name")
    
    # Nest custom function here:
    b_success = eia.eia_download_http_reponse()
    print(f"Status of successful eia_download_http_reponse: {b_success}")
    if b_success:
        return func.HttpResponse(f"{name}: This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. No body received.",
             status_code=200
        )