import json
import time
from urllib.request import Request, urlopen

import gspread
import pandas as pd

monitor_url = 'https://monitor.uruguaysevacuna.gub.uy/plugin/cda/api/doQuery?'

region_letter = {
    "A": "ar", "B": "ca", "C": "cl", "D": "co", "E": "du", "F": "fs", "G": "fd", "H": "la", "I": "ma", "J": "mo",
    "K": "pa", "L": "rn", "M": "rv", "N": "ro", "O": "sa", "P": "sj", "Q": "so", "R": "ta", "S": "tt", "X": "unk"
}


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
    return get_data(data, [
        'code', 'p_first_dose', 'name', 'scale', 'first_dose', 'population', 'second_dose', 'p_second_dose'])


def region_vaccination_centers():
    data = b"path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_filtro_centro_vacuna&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['code', 'name'])


def date_agenda(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_indicadores_gral_agenda&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['future', 'today'])


def date_agenda_second_dose(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunasCovid.cda&" \
           b"dataAccessId=sql_indicadores_gral_agenda_dosis2&outputIndexId=1&pageSize=0&pageStart=0&sortBy=&paramsearchBox="
    return get_data(data, ['future', 'today'])


def today_status(date):
    # Date format YYYYMMDD
    today_str = bytes(date.replace("-", "").encode())
    data = b"paramp_periodo_desde_sk=" + today_str + b"&paramp_periodo_hasta_sk=" + today_str + \
           b"&path=%2Fpublic%2FEpidemiologia%2FVacunas+Covid%2FPaneles%2FVacunas+Covid%2FVacunas" \
           b"Covid.cda&dataAccessId=sql_indicadores_generales&outputIndexId=1&pageSize=0&pageStart=0&" \
           b"sortBy=&paramsearchBox="
    return get_data(data, ['total_vaccinations', 'today_vaccinations', 'first_dose', 'second_dose', 'update_time'])


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
    init_cols = ["daily_vaccinated", "daily_coronavac", "daily_pfizer", "daily_agenda_ini", "daily_agenda",
                 "total_ar", "total_ca", "total_cl", "total_co", "total_du", "total_fd", "total_fs",
                 "total_la", "total_ma", "total_mo", "total_pa", "total_rn", "total_ro", "total_rv", "total_sa",
                 "total_sj", "total_so", "total_ta", "total_tt",
                 "people_ar", "people_ca", "people_cl", "people_co", "people_du", "people_fd", "people_fs",
                 "people_la", "people_ma", "people_mo", "people_pa", "people_rn", "people_ro", "people_rv", "people_sa",
                 "people_sj", "people_so", "people_ta", "people_tt",
                 "fully_ar", "fully_ca", "fully_cl", "fully_co", "fully_du", "fully_fd", "fully_fs",
                 "fully_la", "fully_ma", "fully_mo", "fully_pa", "fully_rn", "fully_ro", "fully_rv", "fully_sa",
                 "fully_sj", "fully_so", "fully_ta", "fully_tt",
                 ]

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


def update():
    # region_vacc_centers = region_vaccination_centers()

    # for region_vacc_center_index, region_vacc_center in region_vacc_centers.iterrows():
    #    name = region_vacc_center["name"]
    #    print(name)
    #    #print(name.split(" ")[0])

    #    # print(region_vacc_center)
    # return False

    updates = False

    daily_vac_origin = daily_vaccinated()

    today = transform_date(daily_vac_origin.tail(1)["date"].values[0])

    day_agenda = int(date_agenda(today)["today"].item() or 0)
    # Increment to the total day agenda the second dose
    day_agenda += int(date_agenda_second_dose(today)["today"].item() or 0)

    today_vac_status = today_status(today)

    today_uodate_time = today + " " + today_vac_status["update_time"].values[0]

    # TODO: The api began to misinform the total, it is calculated with people first and second dose
    # today_total_vaccinations = int(today_vac_status["total_vaccinations"].item() or 0)
    today_total_people_vaccinations = int(today_vac_status["first_dose"].item() or 0)
    today_total_fully_vaccinations = int(today_vac_status["second_dose"].item() or 0)
    today_total_vaccinations = today_total_people_vaccinations + today_total_fully_vaccinations

    gc = gspread.service_account()
    sh = gc.open("CoronavirusUY - Vaccination monitor")

    sheet = sh.worksheet("Uruguay")
    sheet_dic = sheet.get_all_records()
    sheet_headers = sheet.row_values(1)

    daily_people_vaccinated_col_index = get_col_index(sheet_headers, "people_vaccinated")
    daily_people_fully_vaccinated_col_index = get_col_index(sheet_headers, "people_fully_vaccinated")

    daily_vac_total_col_index = get_col_index(sheet_headers, "daily_vaccinated")
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
            time.sleep(2)  # Wait for refresh
            sheet_dic = sheet.get_all_records()  # Get updated changes
            sheet_row = find_row(date_row, sheet_dic)

        sheet_daily_vac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_vaccinated"] or 0)

        sheet_daily_vac_coronavac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_coronavac"] or 0)
        sheet_daily_vac_pfizer = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_pfizer"] or 0)

        daily_vac_origin_value = daily_vac_origin_row["daily_vaccinated"]
        daily_vac_coronavac_origin_value = int(daily_vac_origin_row["daily_coronavac"])
        daily_vac_pfizer_origin_value = int(daily_vac_origin_row["daily_pfizer"])

        sheet_row_index = -1 if len(sheet_row) == 0 else get_row_index(sheet_dic, sheet_row[0])

        sheet_agenda_ini = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda_ini"] or 0)
        sheet_agenda = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_agenda"] or 0)

        sheet_people_vaccinated = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_vaccinated"] or 0)
        sheet_fully_vaccinations = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_fully_vaccinated"] or 0)

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

            # Update People vaccinated and Fully vaccinated
            if sheet_people_vaccinated != today_total_people_vaccinations:
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_people_vaccinated_col_index,
                                        value=today_total_people_vaccinations)
                )
            if sheet_fully_vaccinations != today_total_fully_vaccinations:
                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_people_fully_vaccinated_col_index,
                                        value=today_total_fully_vaccinations)
                )

        if len(sheet_row) == 0:  # Extra control
            print("Create:" + date_row + " old: none new:" + str(daily_vac_origin_value))
        else:
            if sheet_daily_vac != daily_vac_origin_value:

                print("Update:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_daily_vac) + " new:" + str(daily_vac_origin_value))

                if int(daily_vac_origin_value) < sheet_daily_vac:
                    print("* Warning! decrement!")

                if int(daily_vac_origin_value) != sheet_daily_vac:
                    batch_update_cells.append(
                        gspread.models.Cell(sheet_row_index, daily_vac_total_col_index, value=daily_vac_origin_value)
                    )

            if int(daily_vac_coronavac_origin_value) != sheet_daily_vac_coronavac:
                # Update daily vaccinated by type

                print("Update Coronavac:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_daily_vac_coronavac) + " new:" + str(daily_vac_coronavac_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_coronavac_col_index,
                                        value=daily_vac_coronavac_origin_value)
                )

            if int(daily_vac_pfizer_origin_value) != sheet_daily_vac_pfizer:
                print("Update Pfizer:" + date_row + " idx:" + str(sheet_row_index) + " old:" + str(
                    sheet_daily_vac_pfizer) + " new:" + str(daily_vac_pfizer_origin_value))

                batch_update_cells.append(
                    gspread.models.Cell(sheet_row_index, daily_pfizer_col_index,
                                        value=daily_vac_pfizer_origin_value)
                )

            if today == date_row:

                # Get region data for that date
                # TODO: The api lost the filter by date
                daily_vac_region_origin = region_vaccinated(date_row)

                for daily_vac_region_origin_index, daily_vac_region_origin_row in daily_vac_region_origin.iterrows():
                    # Generate the label with the sheet format
                    region_id = daily_vac_region_origin_row["code"].split("-")[1].lower()
                    region_total_label = "total_" + region_id
                    region_people_label = "people_" + region_id
                    region_fully_label = "fully_" + region_id
                    sheet_total_vac_region = 0 if len(sheet_row) == 0 else int(sheet_row[0][region_total_label] or 0)

                    population = int(daily_vac_region_origin_row["population"].replace(".", ""))
                    daily_vac_region_origin_p_first_value = float(
                        daily_vac_region_origin_row["p_first_dose"].replace("%", "").replace(",", ".")) / 100

                    daily_vac_region_origin_p_second_value = float(
                        daily_vac_region_origin_row["p_second_dose"].replace("%", "").replace(",", ".")) / 100

                    daily_vac_region_origin_people_value = int(population * daily_vac_region_origin_p_first_value)
                    daily_vac_region_origin_fully_value = int(population * daily_vac_region_origin_p_second_value)
                    daily_vac_region_origin_total_value = daily_vac_region_origin_people_value
                    daily_vac_region_origin_total_value += daily_vac_region_origin_fully_value

                    if len(sheet_row) == 0:
                        print("Create Region:" + date_row + " " + region_total_label + " old: none new:" + str(
                            daily_vac_region_origin_total_value))
                    elif sheet_total_vac_region != daily_vac_region_origin_total_value:
                        daily_vac_total_col_index = get_col_index(sheet_headers, region_total_label)
                        daily_vac_people_col_index = get_col_index(sheet_headers, region_people_label)
                        daily_vac_fully_col_index = get_col_index(sheet_headers, region_fully_label)

                        print("Update Region:" + date_row + " " + region_total_label + " idx:" + str(
                            sheet_row_index) + " old:" + str(sheet_total_vac_region) + " new:" + str(
                            daily_vac_region_origin_total_value))
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_total_col_index,
                                                value=daily_vac_region_origin_total_value)
                        )

                        # Update people and fully
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_people_col_index,
                                                value=daily_vac_region_origin_people_value)
                        )
                        batch_update_cells.append(
                            gspread.models.Cell(sheet_row_index, daily_vac_fully_col_index,
                                                value=daily_vac_region_origin_fully_value)
                        )
                        if daily_vac_region_origin_total_value < sheet_total_vac_region:
                            print("* Warning! decrement! ")

    to_update = len(batch_update_cells)
    if to_update > 0:
        updates = True
        update_data = sheet.update_cells(batch_update_cells)
        updated = update_data["updatedCells"]
        print("To update cells:" + str(to_update) + " Updated:" + str(updated))

        if to_update != updated:
            # Find difference and force to update
            address = []
            rows = []
            cols = []
            values = []
            for cell in batch_update_cells:
                rows.append(cell.row)
                cols.append(cell.col)
                address.append(cell.address)
                values.append(cell.value)

            ret_values = sheet.batch_get(address)
            force_cells = []
            for index, val in enumerate(ret_values):
                if int(val[0][0]) != values[index]:
                    print("Not updated:" + address[index] + " Val:" + str(values[index]) + " S:" + str(int(val[0][0])))
                    force_cells.append(
                        gspread.models.Cell(rows[index], cols[index], value=values[index])
                    )
            to_update = len(force_cells)
            if to_update > 0:
                update_data = sheet.update_cells(force_cells)
                updated = update_data["updatedCells"]
                print("Force to update cells:" + str(to_update) + " Updated:" + str(updated))

    # Refresh and check results
    time.sleep(2)  # Wait for refresh
    sheet_dic = sheet.get_all_records()  # Get updated changes
    last_row = sheet_dic[-1]
    last_date = last_row["date"]

    if last_date == today:
        print("Source Total:" + str(last_row["total_vaccinations"]) + " Final Total:" + str(today_total_vaccinations))

        # Update date time data
        sheet_data = sh.worksheet("Data")
        sheet_data.update_cell(6, 10, today_uodate_time)

    return updates


if __name__ == "__main__":
    limit_retry = 10
    num_retry = 1
    while num_retry <= limit_retry:
        print("Update:" + str(num_retry))
        if not update():
            print("Update finished")
            break
        print("Updated data, retrying to ensure no pending data...")
        num_retry += 1

    if num_retry > limit_retry:
        print("Retry limit reached")
