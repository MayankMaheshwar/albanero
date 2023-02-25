from flask import Flask, request, make_response
import requests
import io
import zipfile

app = Flask(__name__)


@app.route("/download", methods=["GET"])
def download_csv():
    # Get the CSV file URL and number of rows per small file from the request parameters
    csv_url = request.args.get("csv_url")
    rows_per_file = int(request.args.get("rows_per_file"))

    # Make a request to download the CSV file
    csv_response = requests.get(csv_url, stream=True)

    # Set the content type to 'text/csv' and attachment disposition for the response
    response = make_response()
    response.headers["Content-Type"] = "text/csv"
    response.headers["Content-Disposition"] = "attachment; filename=output.zip"

    # Use a byte buffer to hold the small CSV files
    zip_buffer = io.BytesIO()

    # Create a ZipFile object to hold the small CSV files
    with zipfile.ZipFile(
        zip_buffer, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True
    ) as zip_file:
        # Iterate over the lines in the CSV file
        line_count = 0
        small_file_count = 0
        small_file_buffer = io.StringIO()
        for line in csv_response.iter_lines():
            line_count += 1
            # Write the line to the small file buffer
            small_file_buffer.write(line.decode("utf-8"))
            small_file_buffer.write("\n")
            # If the buffer has reached the maximum number of rows per file, create a new small file
            if line_count % rows_per_file == 0:
                small_file_count += 1
                small_file_buffer.seek(0)
                # Add the small file to the ZipFile object
                zip_file.writestr(
                    f"small_file_{small_file_count}.csv", small_file_buffer.getvalue()
                )
                # Reset the small file buffer
                small_file_buffer = io.StringIO()

        # If there are remaining rows in the small file buffer, create a new small file
        if small_file_buffer.getvalue() != "":
            small_file_count += 1
            small_file_buffer.seek(0)
            # Add the last small file to the ZipFile object
            zip_file.writestr(
                f"small_file_{small_file_count}.csv", small_file_buffer.getvalue()
            )

    # Set the ZipFile object to the response content
    response.data = zip_buffer.getvalue()

    return response


if __name__ == "__main__":
    app.run(debug=True)
