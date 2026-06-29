import json
import logging

from defusedxml import ElementTree


def parse_nbp_xml_to_records(xml_text: str) -> list[dict]:
    """Takes raw XML string and returns a list of flat dictionaries."""
    logger = logging.getLogger(__name__)
    root = ElementTree.fromstring(xml_text)
    rates_list = []
    try:
        effective_date = root.findtext("data_publikacji")
        for child in root.findall("pozycja"):
            rates_list.append(
                {
                    "nazwa_waluty": child.findtext("nazwa_waluty"),
                    "przelicznik": child.findtext("przelicznik"),
                    "kod_waluty": child.findtext("kod_waluty"),
                    "kurs_sredni": child.findtext("kurs_sredni"),
                    "data_publikacji": effective_date,
                },
            )
    except ElementTree.ParseError:
        logger.exception("Parsing error")
        raise
    logger.info("Parsed xml into list")
    logger.info("::group::")
    logger.info("\n".join(json.dumps(r, ensure_ascii=False) for r in rates_list))
    logger.info("::endgroup::")
    return rates_list
