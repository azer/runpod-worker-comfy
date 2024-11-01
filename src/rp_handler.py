import runpod
from runpod.serverless.utils import rp_upload
import json
import urllib.request
import urllib.parse
import time
import os
import requests
import base64
from io import BytesIO
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Time to wait between API check attempts in milliseconds
COMFY_API_AVAILABLE_INTERVAL_MS = 50
# Maximum number of API check attempts
COMFY_API_AVAILABLE_MAX_RETRIES = 500
# Time to wait between poll attempts in milliseconds
COMFY_POLLING_INTERVAL_MS = int(os.environ.get("COMFY_POLLING_INTERVAL_MS", 250))
# Maximum number of poll attempts
COMFY_POLLING_MAX_RETRIES = int(os.environ.get("COMFY_POLLING_MAX_RETRIES", 500))
# Host where ComfyUI is running
COMFY_HOST = "127.0.0.1:8188"
# Enforce a clean state after each job is done
REFRESH_WORKER = os.environ.get("REFRESH_WORKER", "false").lower() == "true"

def validate_input(job_input):
    logger.info(f"Validating input: {job_input}")
    if job_input is None:
        return None, "Please provide input"

    if isinstance(job_input, str):
        try:
            job_input = json.loads(job_input)
        except json.JSONDecodeError:
            return None, "Invalid JSON format in input"

    workflow = job_input.get("workflow")
    if workflow is None:
        return None, "Missing 'workflow' parameter"

    images = job_input.get("images")
    if images is not None:
        if not isinstance(images, list) or not all(
            "name" in image and "image" in image for image in images
        ):
            return None, "'images' must be a list of objects with 'name' and 'image' keys"

    logger.info("Input validation successful")
    return {"workflow": workflow, "images": images}, None

def check_server(url, retries=500, delay=50):
    logger.info(f"Checking server availability: {url}")
    for i in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logger.info("API is reachable")
                return True
        except requests.RequestException as e:
            logger.warning(f"API not reachable, attempt {i+1}/{retries}: {str(e)}")
        time.sleep(delay / 1000)

    logger.error(f"Failed to connect to server at {url} after {retries} attempts.")
    return False

def upload_images(images):
    logger.info("Starting image upload process")
    if not images:
        logger.info("No images to upload")
        return {"status": "success", "message": "No images to upload", "details": []}

    responses = []
    upload_errors = []

    for image in images:
        name = image["name"]
        image_data = image["image"]
        blob = base64.b64decode(image_data)

        files = {
            "image": (name, BytesIO(blob), "image/png"),
            "overwrite": (None, "true"),
        }

        logger.info(f"Uploading image: {name}")
        response = requests.post(f"http://{COMFY_HOST}/upload/image", files=files)
        if response.status_code != 200:
            error_msg = f"Error uploading {name}: {response.text}"
            logger.error(error_msg)
            upload_errors.append(error_msg)
        else:
            success_msg = f"Successfully uploaded {name}"
            logger.info(success_msg)
            responses.append(success_msg)

    if upload_errors:
        logger.warning("Image upload completed with errors")
        return {
            "status": "error",
            "message": "Some images failed to upload",
            "details": upload_errors,
        }

    logger.info("Image upload completed successfully")
    return {
        "status": "success",
        "message": "All images uploaded successfully",
        "details": responses,
    }

def queue_workflow(workflow):
    logger.info("Queueing workflow")
    data = json.dumps({"prompt": workflow}).encode("utf-8")
    req = urllib.request.Request(f"http://{COMFY_HOST}/prompt", data=data)
    response = json.loads(urllib.request.urlopen(req).read())
    logger.info(f"Workflow queued with response: {response}")
    return response

def get_history(prompt_id):
    logger.info(f"Retrieving history for prompt ID: {prompt_id}")
    with urllib.request.urlopen(f"http://{COMFY_HOST}/history/{prompt_id}") as response:
        history = json.loads(response.read())
    logger.info(f"History retrieved: {history}")
    return history

def base64_encode(img_path):
    logger.info(f"Encoding image to base64: {img_path}")
    with open(img_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode("utf-8")
    return encoded_string

def process_output_images(outputs, job_id):
    logger.info(f"Processing output images for job ID: {job_id}")
    COMFY_OUTPUT_PATH = os.environ.get("COMFY_OUTPUT_PATH", "/comfyui/output")
    output_images = {}

    for node_id, node_output in outputs.items():
        if "images" in node_output:
            for image in node_output["images"]:
                output_images = os.path.join(image["subfolder"], image["filename"])

    logger.info("Image generation completed")
    local_image_path = f"{COMFY_OUTPUT_PATH}/{output_images}"
    logger.info(f"Local image path: {local_image_path}")

    if os.path.exists(local_image_path):
        if os.environ.get("BUCKET_ENDPOINT_URL", False):
            image = rp_upload.upload_image(job_id, local_image_path)
            logger.info("Image uploaded to AWS S3")
        else:
            image = base64_encode(local_image_path)
            logger.info("Image converted to base64")

        return {
            "status": "success",
            "message": image,
        }
    else:
        error_msg = f"The image does not exist in the specified output folder: {local_image_path}"
        logger.error(error_msg)
        return {
            "status": "error",
            "message": error_msg,
        }

def handler(job):
    logger.info(f"Received job: {job}")
    job_input = job["input"]

    validated_data, error_message = validate_input(job_input)
    if error_message:
        logger.error(f"Input validation failed: {error_message}")
        return {"error": error_message}

    workflow = validated_data["workflow"]
    images = validated_data.get("images")

    if not check_server(f"http://{COMFY_HOST}", COMFY_API_AVAILABLE_MAX_RETRIES, COMFY_API_AVAILABLE_INTERVAL_MS):
        return {"error": "ComfyUI API is not available"}

    upload_result = upload_images(images)
    if upload_result["status"] == "error":
        logger.error(f"Image upload failed: {upload_result}")
        return upload_result

    try:
        queued_workflow = queue_workflow(workflow)
        prompt_id = queued_workflow["prompt_id"]
        logger.info(f"Queued workflow with ID {prompt_id}")
    except Exception as e:
        error_msg = f"Error queuing workflow: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}

    logger.info(f"Starting image generation polling with interval {COMFY_POLLING_INTERVAL_MS}ms and max retries {COMFY_POLLING_MAX_RETRIES}")
    retries = 0
    start_time = time.time()
    try:
        while retries < COMFY_POLLING_MAX_RETRIES:
            history = get_history(prompt_id)
            elapsed_time = time.time() - start_time

            logger.info(f"Checking history - Retry {retries+1}/{COMFY_POLLING_MAX_RETRIES}, "
                       f"Elapsed time: {elapsed_time:.2f}s")

            if prompt_id in history and history[prompt_id].get("outputs"):
                logger.info(f"Generation completed after {retries} retries and {elapsed_time:.2f} seconds")
                break

            # Log status if available
            if prompt_id in history:
                execution_status = history[prompt_id].get("status", {})
                logger.info(f"Generation in progress - Status: {execution_status}")
            else:
                logger.warning(f"Prompt ID {prompt_id} not found in history")

            time.sleep(COMFY_POLLING_INTERVAL_MS / 1000)
            retries += 1
        else:
            error_msg = f"Max retries ({COMFY_POLLING_MAX_RETRIES}) reached while waiting for image generation. Total time elapsed: {elapsed_time:.2f}s"
            logger.error(error_msg)
            return {"error": error_msg}
    except Exception as e:
        error_msg = f"Error waiting for image generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}

    images_result = process_output_images(history[prompt_id].get("outputs"), job["id"])
    result = {**images_result, "refresh_worker": REFRESH_WORKER}
    logger.info(f"Job completed with result: {result}")
    return result

if __name__ == "__main__":
    logger.info("Starting RunPod handler")
    runpod.serverless.start({"handler": handler})
