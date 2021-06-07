import datetime

import gspread
import pandas as pd

historic_url = 'https://catalogodatos.gub.uy/datastore/dump/5c549ba0-126b-45e0-b43f-b0eea72cf2cf?bom=True'

columns_map = {
    'Montevideo': "mo",
    'Artigas': "ar",
    'Canelones': "ca",
    'Cerro Largo': "cl",
    'Colonia': "co",
    'Durazno': "du",
    'Flores': "fs",
    'Florida': "fd",
    'Lavalleja': "la",
    'Maldonado': "ma",
    'Paysandú': "pa",
    'Río Negro': "rn",
    'Rivera': "rv",
    'Rocha': "ro",
    'Salto': "sa",
    'San José': "sj",
    'Soriano': "so",
    'Tacuarembó': "ta",
    'Treinta y Tres': "tt",
}


def find_row(date, data_dic):
    return [elem for elem in data_dic if elem["date"] == date]


def get_row_index(sheet_dic, row):
    return sheet_dic.index(row) + 2


def get_col_index(headers, label):
    return headers.index(label) + 1


def get_historic():
    return pd.read_csv(historic_url)


def evaluate_row(batch_update_cells, base_value, new_value, row_index, col_index, update_msg):
    if new_value != base_value:
        print(f"{update_msg} idx:{row_index} old:{base_value} new:{new_value}")

        if int(base_value) > new_value:
            print("* Warning! decrement!")

        batch_update_cells.append(
            gspread.models.Cell(row_index, col_index, value=new_value)
        )
    return batch_update_cells


def update():
    gc = gspread.service_account()
    sh = gc.open("CoronavirusUY - Vaccination monitor")

    sheet = sh.worksheet("Uruguay")
    sheet_dic = sheet.get_all_records()
    sheet_headers = sheet.row_values(1)

    daily_vaccinated_col_index = get_col_index(sheet_headers, "daily_vaccinated")
    people_vaccinated_col_index = get_col_index(sheet_headers, "people_vaccinated")
    people_fully_vaccinated_col_index = get_col_index(sheet_headers, "people_fully_vaccinated")

    daily_coronavac_col_index = get_col_index(sheet_headers, "daily_coronavac")
    people_coronavac_col_index = get_col_index(sheet_headers, "people_coronavac")
    fully_coronavac_col_index = get_col_index(sheet_headers, "fully_coronavac")

    daily_pfizer_col_index = get_col_index(sheet_headers, "daily_pfizer")
    people_pfizer_col_index = get_col_index(sheet_headers, "people_pfizer")
    fully_pfizer_col_index = get_col_index(sheet_headers, "fully_pfizer")

    daily_astrazeneca_col_index = get_col_index(sheet_headers, "daily_astrazeneca")
    people_astrazeneca_col_index = get_col_index(sheet_headers, "people_astrazeneca")
    fully_astrazeneca_col_index = get_col_index(sheet_headers, "fully_astrazeneca")

    historic_data = get_historic().iloc[::-1]  # Reverse mode

    historic_result = {}

    first_date_v = historic_data.head(1)["Fecha"].values[0].split("T")[0].split("-")
    first_date = datetime.date(int(first_date_v[0]), int(first_date_v[1]), int(first_date_v[2]))

    total_first_dose = 0
    total_second_dose = 0
    total_sinovac_first_dose = 0
    total_sinovac_second_dose = 0
    total_pfizer_first_dose = 0
    total_pfizer_second_dose = 0
    total_astrazeneca_first_dose = 0
    total_astrazeneca_second_dose = 0

    batch_update_cells = []

    curr_date = first_date

    region_result = {}

    for element_index, element in historic_data.iterrows():

        # date = element["Fecha"].split("T")[0]
        date_row = str(curr_date)

        daily_first_dose = int(element["Total Dosis 1"])
        total_first_dose += daily_first_dose

        daily_second_dose = int(element["Total Dosis 2"])
        total_second_dose += daily_second_dose

        daily_sinovac_first_dose = int(element["1era Dosis Sinovac"])
        total_sinovac_first_dose += daily_sinovac_first_dose

        daily_sinovac_second_dose = int(element["2da Dosis Sinovac"])
        total_sinovac_second_dose += daily_sinovac_second_dose

        daily_pfizer_first_dose = int(element["1era Dosis Pfizer"])
        total_pfizer_first_dose += daily_pfizer_first_dose

        daily_pfizer_second_dose = int(element["2da Dosis Pfizer"])
        total_pfizer_second_dose += daily_pfizer_second_dose

        daily_astrazeneca_first_dose = int(element["1era Dosis Astrazeneca"])
        total_astrazeneca_first_dose += daily_astrazeneca_first_dose

        daily_astrazeneca_second_dose = int(element["2da Dosis Astrazeneca"])
        total_astrazeneca_second_dose += daily_astrazeneca_second_dose

        historic_result[str(curr_date)] = {
            "daily_first_dose": daily_first_dose,
            "daily_second_dose": daily_second_dose,
            "daily_sinovac_first_dose": daily_sinovac_first_dose,
            "daily_sinovac_second_dose": daily_sinovac_second_dose,
            "daily_pfizer_first_dose": daily_pfizer_first_dose,
            "daily_pfizer_second_dose": daily_pfizer_second_dose,
            "daily_astrazeneca_first_dose": daily_astrazeneca_first_dose,
            "daily_astrazeneca_second_dose": daily_astrazeneca_second_dose,

            "total_first_dose": total_first_dose,
            "total_second_dose": total_second_dose,
            "total_sinovac_first_dose": total_sinovac_first_dose,
            "total_sinovac_second_dose": total_sinovac_second_dose,
            "total_pfizer_first_dose": total_pfizer_first_dose,
            "total_pfizer_second_dose": total_pfizer_second_dose,
            "total_astrazeneca_first_dose": total_astrazeneca_first_dose,
            "total_astrazeneca_second_dose": total_astrazeneca_second_dose,
        }

        sheet_row = find_row(str(curr_date), sheet_dic)
        if len(sheet_row) == 0:  # If not exist, create the row
            print(f"Date not found {curr_date}")
        else:
            sheet_row_index = -1 if len(sheet_row) == 0 else get_row_index(sheet_dic, sheet_row[0])

            sheet_daily_vac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_vaccinated"] or 0)
            sheet_people_vaccinated = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_vaccinated"] or 0)
            sheet_fully_vaccinations = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_fully_vaccinated"] or 0)

            sheet_daily_coronavac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_coronavac"] or 0)
            sheet_people_coronavac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_coronavac"] or 0)
            sheet_fully_coronavac = 0 if len(sheet_row) == 0 else int(sheet_row[0]["fully_coronavac"] or 0)

            sheet_daily_pfizer = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_pfizer"] or 0)
            sheet_people_pfizer = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_pfizer"] or 0)
            sheet_fully_pfizer = 0 if len(sheet_row) == 0 else int(sheet_row[0]["fully_pfizer"] or 0)

            sheet_daily_astrazeneca = 0 if len(sheet_row) == 0 else int(sheet_row[0]["daily_astrazeneca"] or 0)
            sheet_people_astrazeneca = 0 if len(sheet_row) == 0 else int(sheet_row[0]["people_astrazeneca"] or 0)
            sheet_fully_astrazeneca = 0 if len(sheet_row) == 0 else int(sheet_row[0]["fully_astrazeneca"] or 0)

            # Exclude the last date (today?)
            if element_index != 0:
                batch_update_cells = evaluate_row(
                    batch_update_cells,
                    sheet_daily_vac, (daily_first_dose + daily_second_dose),
                    sheet_row_index, daily_vaccinated_col_index,
                    f"Update Daily: {date_row}"
                )

                batch_update_cells = evaluate_row(
                    batch_update_cells,
                    sheet_people_vaccinated, total_first_dose,
                    sheet_row_index, people_vaccinated_col_index,
                    f"Update People: {date_row}"
                )

                batch_update_cells = evaluate_row(
                    batch_update_cells,
                    sheet_fully_vaccinations, total_second_dose,
                    sheet_row_index, people_fully_vaccinated_col_index,
                    f"Update Fully: {date_row}"
                )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_daily_coronavac, daily_sinovac_first_dose + daily_sinovac_second_dose,
                sheet_row_index, daily_coronavac_col_index,
                f"Update Daily Coronavac: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_people_coronavac, daily_sinovac_first_dose,
                sheet_row_index, people_coronavac_col_index,
                f"Update People Coronavac: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_fully_coronavac, daily_sinovac_second_dose,
                sheet_row_index, fully_coronavac_col_index,
                f"Update Fully Coronavac: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_daily_pfizer, daily_pfizer_first_dose + daily_pfizer_second_dose,
                sheet_row_index, daily_pfizer_col_index,
                f"Update Daily Pfizer: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_people_pfizer, daily_pfizer_first_dose,
                sheet_row_index, people_pfizer_col_index,
                f"Update People Pfizer: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_fully_pfizer, daily_pfizer_second_dose,
                sheet_row_index, fully_pfizer_col_index,
                f"Update Fully Pfizer: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_daily_astrazeneca, daily_astrazeneca_first_dose + daily_astrazeneca_second_dose,
                sheet_row_index, daily_astrazeneca_col_index,
                f"Update Daily Astrazeneca: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_people_astrazeneca, daily_astrazeneca_first_dose,
                sheet_row_index, people_astrazeneca_col_index,
                f"Update People Astrazeneca: {date_row}"
            )

            batch_update_cells = evaluate_row(
                batch_update_cells,
                sheet_fully_astrazeneca, daily_astrazeneca_second_dose,
                sheet_row_index, fully_astrazeneca_col_index,
                f"Update Fully Astrazeneca: {date_row}"
            )

            # Regions
            for region_key in columns_map.keys():
                region_first_dose_key = region_key + " Dosis 1"
                region_second_dose_key = region_key + " Dosis 2"

                region_first_dosis_value = int(element[region_first_dose_key])
                region_second_dosis_value = int(element[region_second_dose_key])

                sheet_region_key = columns_map[region_key]
                if "people_res_" + sheet_region_key not in region_result:
                    region_result["people_res_" + sheet_region_key] = 0
                region_result["people_res_" + sheet_region_key] += region_first_dosis_value
                if "fully_res_" + sheet_region_key not in region_result:
                    region_result["fully_res_" + sheet_region_key] = 0
                region_result["fully_res_" + sheet_region_key] += region_second_dosis_value

                people_region_col_index = get_col_index(sheet_headers, "people_res_" + sheet_region_key)
                fully_region_col_index = get_col_index(sheet_headers, "fully_res_" + sheet_region_key)

                sheet_region_people = 0 if len(sheet_row) == 0 else int(
                    sheet_row[0]["people_res_" + sheet_region_key] or 0)
                sheet_region_fully = 0 if len(sheet_row) == 0 else int(
                    sheet_row[0]["fully_res_" + sheet_region_key] or 0)

                batch_update_cells = evaluate_row(
                    batch_update_cells,
                    sheet_region_people, region_result["people_res_" + sheet_region_key],
                    sheet_row_index, people_region_col_index,
                    f"Update People {region_key} {sheet_region_key}: {date_row}"
                )

                batch_update_cells = evaluate_row(
                    batch_update_cells,
                    sheet_region_fully, region_result["fully_res_" + sheet_region_key],
                    sheet_row_index, fully_region_col_index,
                    f"Update Fully {region_key} {sheet_region_key}: {date_row}"
                )

        curr_date = curr_date + datetime.timedelta(days=1)

    to_update = len(batch_update_cells)
    if to_update > 0:
        update_data = sheet.update_cells(batch_update_cells)
        updated = update_data["updatedCells"]
        print("To update cells:" + str(to_update) + " Updated:" + str(updated))


if __name__ == "__main__":
    update()
