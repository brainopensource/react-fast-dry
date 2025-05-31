from fastapi.responses import JSONResponse, FileResponse
from fastapi import status

class APIResponse:
    @staticmethod
    def success(data=None, message="Success", status_code=status.HTTP_200_OK):
        return JSONResponse(content={"status": "success", "message": message, "data": data}, status_code=status_code)

    @staticmethod
    def file(filepath, filename, message="Success", status_code=status.HTTP_200_OK):
        headers = {"X-Message": message}
        return FileResponse(filepath, filename=filename, headers=headers, status_code=status_code)

    @staticmethod
    def error(message="An error occurred", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR):
        return JSONResponse(content={"status": "error", "message": message}, status_code=status_code)

    @staticmethod
    def not_found(message="Not found"): 
        return JSONResponse(content={"status": "error", "message": message}, status_code=status.HTTP_404_NOT_FOUND)

    @staticmethod
    def bad_request(message="Bad request"):
        return JSONResponse(content={"status": "error", "message": message}, status_code=status.HTTP_400_BAD_REQUEST)
