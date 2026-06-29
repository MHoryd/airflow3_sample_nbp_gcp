import httpx


def get_target_string(request_for_rates_list: httpx.Response, run_date: str) -> str:
    result_list = request_for_rates_list.text.splitlines()
    target = next(
        (i for i in result_list if i[-6:] == run_date and i[0] == "a"),
        None,
    )
    return target
