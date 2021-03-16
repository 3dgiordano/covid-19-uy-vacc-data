import json
import time
from urllib.request import Request, urlopen

import gspread
import pandas as pd

monitor_url = 'https://monitor.uruguaysevacuna.gub.uy/plugin/cda/api/doQuery?'


def find_row(date, data_dic):
    return [elem for elem in data_dic if elem["date"] == date]


def get_row_index(sheet_dic, row):
    return sheet_dic.index(row) + 2


def get_col_index(headers, label):
    return headers.index(label) + 1


def get_data(data, columns):
    json_origin = json.loads(urlopen(Request(monitor_url, data=data)).read().decode())
    return pd.DataFrame(json_origin["resultset"], columns=columns).fillna(0)


def daily_vaccinated():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_evolucion&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['date', 'daily_vaccinated', 'daily_coronavac', 'daily_pfizer'])


def region_vaccinated(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&paramp_ncto_desde_sk=0&" \
           b"paramp_ncto_hasta_sk=0&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2F" \
           b"VacunasCovid.cda&dataAccessId=sql_vacunas_depto_vacunatorio&outputIndexId=1&pageSize=0&" \
           b"pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['code', 'total_vaccinated', 'name', 'scale'])


def date_agenda(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_indicadores_gral_agenda&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['future', 'today'])


def today_status(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunas" \
           b"Covid.cda&dataAccessId=sql_indicadores_generales&outputIndexId=1&pageSize=0&pageStart=0&" \
           b"sortBy=&paramsearchBox="
    return get_data(data, ['total_vaccinations', 'today_vaccinations', 'centers', 'update_time'])


def add_formatted_row(spreadsheet, sheet, date):
    sheet_id = sheet.id
    last_row = len(list(filter(None, sheet.col_values(1))))
    last_col = sheet.col_count
    body = {
        'requests': [
            {
                'copyPaste': {
                    'source': {
                        'sheetId': sheet_id,
                        'startRowIndex': int(last_row - 1), 'endRowIndex': last_row,
                        'startColumnIndex': 0, 'endColumnIndex': last_col
                    },
                    'destination': {
                        'sheetId': sheet_id,
                        'startRowIndex': last_row, 'endRowIndex': int(last_row + 1),
                        'startColumnIndex': 0, 'endColumnIndex': last_col
                    },
                    'pasteType': 'PASTE_FORMULA'
                }
            }
        ]
    }
    spreadsheet.batch_update(body)  # Paste Formula
    body["requests"][0]["copyPaste"]["pasteType"] = "PASTE_FORMAT"
    spreadsheet.batch_update(body)

    sheet_headers = sheet.row_values(1)
    date_index = get_col_index(sheet_headers, "date")
    sheet.update_cell(int(last_row + 1), date_index, date)
    init_cols = ["daily_vaccinated", "daily_coronavac", "daily_pfizer", "daily_ar", "daily_agenda_ini", "daily_agenda",
                 "daily_ar", "daily_ca", "daily_cl", "daily_co", "daily_du", "daily_fd", "daily_fs",
                 "daily_la", "daily_ma", "daily_mo", "daily_pa", "daily_rn", "daily_ro", "daily_rv", "daily_sa",
                 "daily_sj", "daily_so", "daily_ta", "daily_tt"]

    batch_update_cells = []
    for col_ini in init_cols:
        col_init_index = get_col_index(sheet_headers, col_ini)
        batch_update_cells.append(gspread.models.Cell(int(last_row + 1), col_init_index, value=0))
    if len(batch_update_cells) > 0:
        sheet.update_cells(batch_update_cells)


def transform_date(date_str):
    date_str = "-".join(date_str.split("-")[::-1])
    if not date_str.startswith("2021-"):
        date_str = "2021-" + date_str  # WA when format is without year
    return date_str


def main():
    daily_vac_origin = daily_vaccinated()

    today = transform_date(daily_vac_origin.tail(1)["date"].values[0])

    day_agenda = int(date_agenda(today)["today"].item() or 0)

    today_vac_status = today_status(today)

    today_total_vaccinations = int(today_vac_status["total_vaccinations"].item() or 0)

    gc = gspread.service_account()
    sh = gc.open("CoronavirusUY - Vaccination monitor")

    sheet = sh.worksheet("Uruguay")
    sheet_dic = sheet.get_all_records()
    sheet_headers = sheet.row_values(1)

    daily_vac_col_index = get_col_index(sheet_headers, "daily_vaccinated")
    daily_coronavac_col_index = get_col_index(sheet_headers, "daily_coronavac")
    daily_pfizer_col_index = get_col_index(sheet_headers, "daily_pfizer")

    daily_agenda_ini_col_index = get_col_index(sheet_headers, "daily_agenda_ini")
    daily_agenda_col_index = get_col_index(sheet_headers, "daily_agenda")

    last_row = sheet_dic[-1]
    last_date = last_row["date"]

    if last_date == today:
        if (today_total_vaccinations - last_row["total_vaccinations"]) < -1:
            print("* Execution Excluded! Corrupt source data? Last valid:" + str(
                last_row["total_vaccinations"]) + " new:" + str(today_total_vaccinations))
            return

    batch_update_cells = []

    for daily_vac_origin_index, daily_vac_origin_row in daily_vac_origin.iterrows():

        # Get date
        date_row = transform_date(daily_vac_origin_row["date"])

        sheet_row = find_row(date_row, sheet_dic)
        if len(sheet_row) == 0:  # If not exist, create the row
            add_formatted_row(sh, sheet, date_row)
            time.sleep(1)  # Wait for refresh
            sheet_dic = sheet.get_all_records()  # Get updated changes
            sheet_row = find_row(date_row, sheet_dic)

        sheet_daily_vac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_vaccinated"] or 0)
        daily_vac_origin_value = daily_vac_origin_row["daily_vaccinated"]

        sheet_row_index = -1 if len(sheet_row) == 0 else get_row_index(sheet_dic, sheet_row[0])

        sheet_agenda_ini = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_ini"] or 0)
        sheet_agenda = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda"] or 0)

        if today == date_row:
            if sheet_agenda_ini == 0 and day_agenda > 0:
                # Set the ini agenda value
                print("Update Agenda ini:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda_ini) + " new:" + str(day_agenda))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_ini_col_index, value=day_agenda)
                )
            if sheet_agenda < day_agenda:
                print("Update Agenda:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_agenda) + " new:" + str(day_agenda))
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_agenda_col_index, value=day_agenda)
                )

        record = True
        if len(sheet_row) == 0:  # Extra control
            print("Create:" + date_row + " old: none new:" + str(daily_vac_origin_value))
        elif sheet_daily_vac != daily_vac_origin_value:

            print("Update:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                sheet_daily_vac) + " new:" + str(daily_vac_origin_value))

            if int(daily_vac_origin_value) < sheet_daily_vac:
                print("* Warning! decrement!")

            if int(daily_vac_origin_value) != sheet_daily_vac:
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_vac_col_index, value=daily_vac_origin_value)
                )

                # Update daily vaccinated by type
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_coronavac_col_index,
                                        value=daily_vac_origin_row["daily_coronavac"])
                )
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_pfizer_col_index,
                                        value=daily_vac_origin_row["daily_pfizer"])
                )
        else:
            record = False

        if record:  # Only request to recalculate regions when daily_vaccinated change
            # time.sleep(60)  # Modification quota every 60 seconds
            # Get region data for that date
            daily_vac_region_origin = region_vaccinated(date_row)

            for daily_vac_region_origin_index, daily_vac_region_origin_row in daily_vac_region_origin.iterrows():
                # Generate the label with the sheet format
                region_label = "daily_" + daily_vac_region_origin_row["code"].split("-")[1].lower()
                sheet_daily_vac_region = 0 if len(sheet_row) == 0 else int(sheet_row[0][region_label] or 0)
                daily_vac_region_origin_value = int(daily_vac_region_origin_row["total_vaccinated"].replace(".", ""))
                if len(sheet_row) == 0:
                    print("Create Region:" + date_row + " " + region_label + " old: none new:" + str(
                        daily_vac_region_origin_value))
                elif sheet_daily_vac_region != daily_vac_region_origin_value:
                    daily_vac_col_index = get_col_index(sheet_headers, region_label)
                    print("Update Region:" + date_row + " " + region_label + " idx:" + str(
                        sheet_row_index) + " old:" + str(sheet_daily_vac_region) + " new:" + str(
                        daily_vac_region_origin_value))
                    batch_update_cells.append(
                        gspread.models.Cell(sheet_row_index, daily_vac_col_index, value=daily_vac_region_origin_value)
                    )
                    if daily_vac_region_origin_value < sheet_daily_vac_region:
                        print("* Warning! decrement! ")

    if len(batch_update_cells) > 0:
        sheet.update_cells(batch_update_cells)

    # Refresh and check results
    sheet_dic = sheet.get_all_records()  # Get updated changes
    last_row = sheet_dic[-1]
    last_date = last_row["date"]

    if last_date == today:
        print("Source Total:" + str(last_row["total_vaccinations"]) + " Final Total:" + str(today_total_vaccinations))


if __name__ == "__main__":
    main()
