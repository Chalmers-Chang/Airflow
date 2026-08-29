from parsers.crew_report import parse_crew_report_pdf

PARSERS = {
    "crew_report": parse_crew_report_pdf,
}


def parse_file(parser_name, path, source):
    if parser_name not in PARSERS:
        raise ValueError("Unknown parser: {0}".format(parser_name))
    return PARSERS[parser_name](path, source)
