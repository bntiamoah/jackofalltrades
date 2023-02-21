



def process_region(zip_file):
    ""


def get_regional_zip_files(main_zip_file):
    return


if __name__ == '__main__':
    main_zip_file = 'xml.zip'

    regional_zips = get_regional_zip_files(main_zip_file)

    if not regional_zips or len(regional_zips) == 0:

        for regional_zip in regional_zips:
            process_region(regional_zip)

