import concurrent.futures
import os
import shutil
import tempfile
import time
import urllib.request as req

from PIL import Image, ImageStat
from PIL import ImageChops

base_url = "https://docs.google.com/spreadsheets/d/e/"
sheet = "2PACX-1vRSB3_JCKkvYQkgEwYW0PkzMJDovwvMwX28B5ainGuDirimi6n4n1nryc0Pbb0fHCfsZVYAnqobgP8D"


def get_tempfile_name(some_id):
    return os.path.join(tempfile.gettempdir(), next(tempfile._get_candidate_names()) + "_" + some_id)


def save_img(oid):
    img_url = f"{base_url}{sheet}/pubchart?oid={oid}&format=image"

    temp_f = get_tempfile_name(".png")
    if os.path.exists(temp_f):
        os.remove(temp_f)
    req.urlretrieve(img_url, temp_f)

    image_file = os.path.abspath(f"../web/charts/{oid}.png")

    image_org = Image.open(image_file)
    image_new = Image.open(temp_f)

    diff = ImageChops.difference(image_org, image_new)

    image_org.close()
    image_new.close()

    stat = ImageStat.Stat(diff)

    diff_ratio = (sum(stat.mean) / (len(stat.mean) * 255))
    print(diff_ratio)
    if diff_ratio != 0:
        shutil.move(temp_f, image_file)

    if os.path.exists(temp_f):
        os.remove(temp_f)


images = [
    "574263984", "1320291746",
    "1201195179", "1744392307",
    "857919432", "425905901",
    "851362461", "395420450",
    "1329486679", "1492441660",
    "827148403", "22096209",
    "1848022003", "873256307",
    "2077796175", "1058712731",
    "1978363820", "1357338484",
    "1038688506", "591742088",
    "603335823", "1958520312",
    "1683681566", "373318070",
    "2063902375", "1924052371",
    "1821951025", "1074834619",
    "1322547223", "682972572",
    "731574492", "119707745",
    "842189614", "1158305404",
    "1047100711", "1781225090",
    "132326038", "259061157",
    "122662822", "121562673",
    "987529461", "1299383115",
    "454080210", "1279668502",
    "105471492", "1092961723",
    "2095693594", "134138183",
    "648030237", "861619954",
    "1720013265", "736411819",
    "1189791692", "824007235",
    "4849953", "1785941673",
    "784232676", "661817159",
    "1133283679", "1140302154",
    "1880002572", "1515424457",
    "1740954658", "655562320",
    "446851537", "1541211770",
    "75622886", "1656096582",
    "1972410624", "770096093",
    "1466475246", "1082865934",
    "1898654916", "1598348414",
    "1576723715", "519951936",
    "205213491", "404093750",
    "1270929451", "1047203349",
    "10315580",
    "626919126",
    "501510119",
    "380999305",
    "1506990494",
    "943690507",
    "1778597259",
    "1029004131",
    "2006897410",
    "1087191394",
    "1617072188",
    "1745356284",
    "1156295619",
    "1908226097",
    "1481111761",
    "2049614015",
    "1168479548",
    "1906852652",
    "1905047399",

    "494905331",
    "1979752349",

    "286695041", "1914304220",
    "2049766195", "1347085705",

    "1984756315", "1512231803",

    "1516739352", "1498232647",

    "1213501245", "419313606",

    # From here they need to always be at the end (texts with function calls)
    "1939554456", "141578891",
    "744871918", "2074125212",

]

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(save_img, img): img for img in images}

    for future in concurrent.futures.as_completed(futures):
        image_arg = futures[future]
        try:
            result = future.result()
        except Exception as exc:
            print("Saving image {} generated an exception: {}".format(image_arg, exc))
        else:
            print("Image {} saved successfully.".format(image_arg))

# Paused to give git time to recognize changes
time.sleep(2)
