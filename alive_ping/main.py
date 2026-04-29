from henchman_sdk import log_info, set_result

log_info("I'm alive")
set_result({
    "_outcome": {
        "code": "alive",
        "status": "success",
        "message": "I'm alive",
    }
})
