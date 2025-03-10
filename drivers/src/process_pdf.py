####################################
# Author: Jon Willinger
# Date: 2025-02-18
# Notes: 
####################################

import os, csv, re
import logging
import pathlib as path
import pymupdf4llm
import tempfile
import pandas as pd
from dateutil import parser

def file_to_byte_array(input_file_path):
    ret = None
    try:
        with open(input_file_path, "rb") as file:
            file.close()
    except FileNotFoundError as e: print(e)
    except IOError as e: print(e) 
    return ret

def check_file_encoding(input_file_path):
    ret = ""
    try:
        with open(input_file_path, 'r', encoding="utf-8") as f:
            content = f.read()
            ret = "utf-8"
    except UnicodeDecodeError as e:
        print("Error: Could not decode file Unicode encoding.")
        print(e)
        try:
            with open(input_file_path, 'r', encoding="latin-1") as f:
                content = f.read()
                ret = "latin-1"
        except Exception:
            print("Error: Could not decode file with Latin-1 encoding either.")
    return ret

def process_pdf_return_data(input_file, output_file, pdf_Name):

    def _process_pdf_to_text_output(input_file, output_file):
        md_text = pymupdf4llm.to_markdown(input_file)
        path.Path(output_file).write_bytes(md_text.encode())
        return output_file

    def _get_df_from_processed_text_file(text_file, input_file, pdf_Name):

        def __process_header(line, header, header_contact):
            print(line);
            contact_line = line.replace("**", "").replace("_", "")[0:40]
            b = False
            if line[0:2] == "# " and "DRIVERS" in line:
                if ("PE" in line.replace(" ", "").upper() or "POLYETHYLENE" in line.replace(" ", "").upper()) and "PET" not in line.replace(" ", "").upper():
                    header = "PE DRIVERS"
                    b = True
                elif "PP" in line.replace(" ", "").upper() or "POLYPROPYLENE" in line.replace(" ", "").upper(): 
                    header = "PP DRIVERS"
                    b = True
                elif "PS" in line.replace(" ", "").upper() or "POLYSTYRENE" in line.replace(" ", "").upper(): 
                    header = "PS DRIVERS"
                    b = True
                elif "ABS" in line.replace(" ", "").upper(): 
                    header = "ABS DRIVERS"
                    b = True
                elif "PVC" in line.replace(" ", "").upper(): 
                    header = "PVC DRIVERS"
                    b = True
                elif "PC" in line.replace(" ", "").upper() or "POLYCARBONATE" in line.replace(" ", "").upper(): 
                    header = "PC DRIVERS"
                    b = True
                elif "PA66" in line.replace(" ", "").upper(): 
                    header = "PA66 DRIVERS"
                    b = True
                elif "PA6" in line.replace(" ", "").upper(): 
                    header = "PA6 DRIVERS"
                    b = True
                elif "PET" in line.replace(" ", "").upper():
                    header = "PET DRIVERS" 
                    b = True
            elif "For additional" in contact_line or "Outlook and Suggested" in contact_line:
                b = False
                if "For additional PE information" in contact_line or "# RTi PE Outlook and Suggested" in contact_line:
                    header_contact = "PE DRIVERS"
                    b = False
                elif "For additional PP information" in contact_line or "# RTi PP Outlook and Suggested" in contact_line:
                    header_contact = "PP DRIVERS"
                    b = False
                elif "For additional PS information" in contact_line or "# RTi PS Outlook and Suggested" in contact_line:
                    header_contact = "PS DRIVERS"
                    b = False
                elif "For additional ABS information" in contact_line or "# RTi ABS Outlook and Suggested" in contact_line:
                    header_contact = "ABS DRIVERS"
                    b = False
                elif "For additional PC information" in contact_line or "# RTi PC Outlook and Suggested" in contact_line:
                    header_contact = "PC DRIVERS"
                    b = False
                elif "For additional PVC information" in contact_line or "# RTi PVC Outlook and Suggested" in contact_line:
                    header_contact = "PVC DRIVERS"
                    b = False
                elif "For additional PA66 information" in contact_line or "For additional Nylon PA66 information" in contact_line or "# RTi PA66 Outlook and Suggested" in contact_line:
                    header_contact = "PA66 DRIVERS"
                    b = False
                elif "For additional PA6 information" in contact_line or "For additional Nylon PA6 information" in contact_line or "For additional Nylon information" in contact_line or \
                "# RTi PA6 Outlook and Suggested" in contact_line:
                    header_contact = "PA6 DRIVERS"
                    b = False
                elif "For additional PET information" in contact_line or "# RTi PET Outlook and Suggested" in contact_line:
                    header_contact = "PET DRIVERS"
                    b = False
            else:
                header = ""
                b = False
            return (header, header_contact, b)

        def __process_date(line, date, b_date): 
            print(line)
            if line[0:4] == "### " or line[0:5] == "#### " or line[0:6] == "##### " or line[0:7] == "###### ":
                b_date_test = False
                for year in range(2015, 2035, 1):
                    year_ = str(year)
                    if year_ in line: 
                        b_date_test = True
                        break
                if b_date_test:
                    if year_ in line: 
                        try:
                            line_ = line.replace("###### ", "").replace("##### ", "").replace("#### ", "").replace("### ", "").replace("\n", "")
                            date_ = parser.parse(line_)
                            b_date = True
                        except ValueError as e:
                            date_ = None
                            b_date = False
                else:
                    date_ = ""
                    b_date = False
            else:
                date_ = ""
                b_date = False
            
            return date_, b_date

        def __process_page(line, page):
            print(line)
            if line[0:5] == "-----":
                page+=1
                b_page = True
            else:
                pass
                b_page = False
            return page, b_page
        
        def __post_process_data_into_df(pages, headers, dates, pdf_Name, b_same_dates):
            pages = pages[:-1]
            headers_ = "headers"
            try:
                if b_same_dates and len(pages) != len(dates):
                    dates = [dates[0] for i in range(0, len(pages))]
                df = pd.DataFrame(data={"pages":pages, headers_:headers, "dates":dates})
            except Exception:
                df = pd.DataFrame(data={"pages":pages, headers_:[f"Error{i}" for i in range(0, len(pages))], "dates":[None for i in range(0, len(pages))]})

            s_header = df[[headers_]].groupby(by=[headers_])[[headers_]].count()[headers_]
            index_ = df[df[headers_].isin(s_header[s_header>1].index.to_list())].index
            df.loc[index_, (headers_)] = ""
            
            for i in range(0, df[headers_].shape[0]):
                if df[headers_].iloc[i] == "": df[headers_].iloc[i] = f"Error{i}"
            
            df["pdfName"] = pdf_Name
            print(df)

            return df

        enc = check_file_encoding(input_file)
        header = ""; header_contact = ""
        with open(text_file, "r", encoding=enc) as infile:
        
            # intialize:
            date = ""; b_date = False; b_page = False
            headers = []; dates = []; pages = []

            for i_row, line in enumerate(infile):
                
                if i_row == 0:
                    page = 1; b_page = True

                if b_page: 
                    pages.append(page)
                    b_get_page_header_data = True
                    b_get_page_date_data = True
                    b_page = False
                    if (len(pages)-len(headers)) > 1: headers.append(header_contact)
                    header_contact = ""; header = ""; b_header = False # reset

                try:
                    if b_get_page_header_data: header, header_contact, b_header = __process_header(line, header, header_contact)
                    if b_get_page_date_data: date, b_date = __process_date(line, date, b_date)
                    page, b_page = __process_page(line, page)
                except Exception: pass
                
                if b_header:
                    headers.append(header)
                    header=""; b_header = False
                    b_get_page_header_data = False
                if b_date:
                    dates.append(date)
                    b_date = False
                    b_get_page_date_data = False
            
            infile.close()
        df = __post_process_data_into_df(pages=pages, headers=headers, 
                                       dates=dates, pdf_Name=pdf_Name, b_same_dates=True)
        
        return df
    
    text_file = _process_pdf_to_text_output(input_file, output_file)
    df = _get_df_from_processed_text_file(text_file, input_file, pdf_Name)
    return df

if __name__ == "__main__":

    pass
    # with tempfile.NamedTemporaryFile(prefix=input_file[:-4], suffix=".md", mode="a", delete=False) as tempfile:
    #     output_file = tempfile.name
    # pdf_Name = "RtiDriver.pdf"
    # df = process_pdf_return_data(os.path.join(filepath, input_file), output_file, pdf_Name)

    