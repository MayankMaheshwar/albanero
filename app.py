from flask import Flask, request, make_response
import requests
import io
import zipfile

app = Flask(__name__)


@app.route("/download", methods=["GET"])
def download_csv():
    csv_url = request.args.get("csv_url")
    rows_per_file = int(request.args.get("rows_per_file"))

    csv_response = requests.get(csv_url, stream=True)

    response = make_response()
    response.headers["Content-Type"] = "text/csv"
    response.headers["Content-Disposition"] = "attachment; filename=output.zip"

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(
        zip_buffer, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True
    ) as zip_file:
        line_count = 0
        small_file_count = 0
        small_file_buffer = io.StringIO()
        for line in csv_response.iter_lines():

            line_count += 1
            small_file_buffer.write(line.decode("utf-8"))
            small_file_buffer.write("\n")
            if line_count % rows_per_file == 0:
                small_file_count += 1
                small_file_buffer.seek(0)
                zip_file.writestr(
                    f"small_file_{small_file_count}.csv", small_file_buffer.getvalue()
                )
                small_file_buffer = io.StringIO()

        if small_file_buffer.getvalue() != "":
            small_file_count += 1
            small_file_buffer.seek(0)
            zip_file.writestr(
                f"small_file_{small_file_count}.csv", small_file_buffer.getvalue()
            )

    response.data = zip_buffer.getvalue()

    return response


if __name__ == "__main__":
    app.run(debug=True)
