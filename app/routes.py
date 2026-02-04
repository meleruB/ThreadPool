import ast

from flask import request, jsonify

from app import webserver, logger
from app.task import TaskType, Task


# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        logger.info("got data in post %s", data)

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)

    # Method Not Allowed
    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    logger.info("JobID is %s", job_id)

    file_path = f"./results/{job_id}"
    if webserver.tasks_runner.is_task_done(int(job_id)):
        with open(file_path) as f:
            output = f.read()
            data = ast.literal_eval(output)
            logger.info("Result is ok for job id %s", job_id)
            return jsonify({'status': 'done',
                            'data': data})
    else:
        logger.info("Task is running for job id %s", job_id)
        return jsonify({'status': 'running'})


@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.STATES_MEAN, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("states mean job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.STATE_MEAN, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("state mean job id = %s", job_id)

    return jsonify({"job_id": job_id})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.BEST_5, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("best_5 job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.WORST_5, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("worst_5 job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.GLOBAL_MEAN, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("global_mean job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.DIFF_FROM_MEAN, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("diff_from_mean job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.STATE_DIFF_FROM_MEAN, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("state_diff_from_mean job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.MEAN_BY_CATEGORY, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("mean_by_category job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    data = request.json
    logger.info("Got request %s", data)

    task = Task(TaskType.STATE_MEAN_BY_CATEGORY, webserver.data_ingestor, data)
    job_id = webserver.tasks_runner.add_task(task)
    logger.info("state_mean_by_category job id = %s", job_id)

    return jsonify({"job_id": job_id})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown_request():
    webserver.tasks_runner.graceful_shutdown()

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    res = webserver.tasks_runner.jobs()

    return jsonify({"status": "done", "jobs": res})

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    res = webserver.tasks_runner.num_jobs()

    return jsonify({"status": "done", "num": res})
# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
