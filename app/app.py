import streamlit as st
st.set_page_config(page_title=None, page_icon=None, layout="wide", initial_sidebar_state="collapsed", menu_items=None)

import os
import tempfile
import logging
from streamlit_navigation_bar import st_navbar
from logging_config import setup_logging
from clifpy import Adt, Hospitalization, Labs, Patient, Position, PatientAssessments, RespiratorySupport, Vitals, MedicationAdminContinuous
from pages._3_adt_qc import show_adt_qc
from pages._4_hosp_qc import show_hosp_qc
from pages._5_labs_qc import show_labs_qc
from pages._6_med_qc import show_meds_qc
# from pages._7_microbio_qc import show_microbio_qc
from pages._8_patient_qc import show_patient_qc
from pages._9_patient_assess_qc import show_patient_assess_qc
from pages._10_position_qc import show_position_qc
from pages._11_resp_qc import show_respiratory_support_qc
from pages._12_vitals_qc import show_vitals_qc



def show_home():
    # Initialize logger
    setup_logging()
    logger = logging.getLogger(__name__)

    _, main_qc_form, _ = st.columns([1, 3, 1])

    with main_qc_form:

        st.title("Quality Controls")

        with st.form(key='main_form', clear_on_submit=True):

            st.write("""
                    Welcome to LightHouse! Our Quality Controls feature is designed to streamline your data validation processes, reduce errors, and ensure that your data is always research-ready. QCs are available for:
                        """)
            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                    st.write("""
                    - **ADT**
                    - **Hospitalization**
                    """)
            with c2:
                    st.write("""
                    - **Labs**
                    - **Medication**
                    """)
            with c3:
                    st.write("""
                    - **Patient**
                    - **Patient Assessment**
                    """)
            with c4:
                    st.write("""
                    - **Position**
                    - **Respiratory Support**
                    """)
            with c5:
                    st.write("""
                    - **Vitals**
                    """)
            
            files = st.file_uploader(
                "Select one or more files", 
                accept_multiple_files=True, 
                type=["csv", "parquet"]
            )

            # Sampling option
            s_col1, _, _, _ = st.columns(4)
            with s_col1:
                sampling_option = st.number_input("Set dataset sample(%) for QC ***(optional)***", min_value=1, max_value=100, value=None, step=5)
            download_path = st.text_input("Enter path to save automated downloads of generated tables and images ***(optional)***", value=None)

            submit = st.form_submit_button(label='Submit')

        if submit:
            st.info("Note that a new tab will not load until the current tab has finished loading. " \
                "The overall progress of the quality control checks will be displayed. For detailed progress information, please expand the required table in the QC section.", 
                icon="ℹ️")
            with st.spinner('Loading...'):
                if files:
                    st.session_state["files"] = "Yes"
                    try:
                        for file in files:
                            logger.info(f"Processing file: {file.name}")
                            
                            # Extract table name and file type
                            table_name = file.name.split('.')[0]
                            filetype = file.name.split('.')[-1]
                            
                            # Map table names to clifpy classes
                            table_class_map = {
                                'clif_adt': Adt,
                                'adt': Adt,
                                'clif_hospitalization': Hospitalization,
                                'hospitalization': Hospitalization,
                                'clif_labs': Labs,
                                'labs': Labs,
                                'clif_patient': Patient,
                                'patient': Patient,
                                'clif_position': Position,
                                'position': Position,
                                'clif_patient_assessments': PatientAssessments,
                                'patient_assessments': PatientAssessments,
                                'clif_respiratory_support': RespiratorySupport,
                                'respiratory_support': RespiratorySupport,
                                'clif_vitals': Vitals,
                                'vitals': Vitals,
                                'clif_medication_admin_continuous': MedicationAdminContinuous,
                                'medication_admin_continuous': MedicationAdminContinuous
                            }
                            
                            if table_name.lower() in table_class_map:
                                logger.info(f"Loading {table_name} using clifpy")
                                
                                # Create temporary directory and save file
                                with tempfile.TemporaryDirectory() as temp_dir:
                                    # Create expected filename format for clifpy
                                    base_table_name = table_name.lower().replace('clif_', '')
                                    temp_file_path = os.path.join(temp_dir, f"clif_{base_table_name}.{filetype}")
                                    
                                    # Write uploaded file content to temporary file
                                    with open(temp_file_path, 'wb') as temp_file:
                                        temp_file.write(file.getvalue())
                                    
                                    # Get the appropriate clifpy class
                                    table_class = table_class_map[table_name.lower()]
                                    
                                    # Load using clifpy from_file method
                                    table_instance = table_class.from_file(
                                        data_directory=temp_dir,
                                        filetype=filetype,
                                        timezone="UTC"
                                    )
                                    
                                    # Store the dataframe in session state
                                    df = table_instance.df
                                    st.session_state[table_name] = df
                                    logger.info(f"Successfully loaded {table_name} using clifpy - shape: {df.shape}")
                                    logger.info(f"Stored in session_state['{table_name}']. Keys: {list(st.session_state.keys())}")
                            else:
                                logger.warning(f"Table {table_name} not supported by clifpy, skipping")
                                
                    except Exception as e:
                        st.write("Error: No files were submitted or an issue occurred while processing the files.")
                        st.write(f"Details: {e}")

                st.session_state['sampling_option'] = None
                if sampling_option:
                    logger.info(f"Sampling option selected: {sampling_option}")
                    st.session_state['sampling_option'] = sampling_option
                
                st.session_state['download_path'] = None
                if download_path:
                    logger.info(f"Download path option selected: {download_path}")
                    st.session_state['download_path'] = download_path

            logger.info("Loading QC results page")
            # tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(["ADT", 
            #     "Hospitalization", "Labs", "Medication", "Microbiology", "Patient", 
            #     "Patient Assessment", "Position", "Respiratory Support", "Vitals"])
            tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs(["ADT", 
                "Hospitalization", "Labs", "Medication", "Patient", 
                "Patient Assessment", "Position", "Respiratory Support", "Vitals"])

            with tab1:
                try:
                    show_adt_qc()
                except Exception as e:
                    st.write(f"Error loading ADT QC: {e}")

            with tab2:
                try:
                    show_hosp_qc()
                except Exception as e:
                    st.write(f"Error loading Hospitalization QC: {e}")

            with tab3:
                try:
                    show_labs_qc()
                except Exception as e:
                    st.write(f"Error loading Labs QC: {e}")

            with tab4:
                try:
                    show_meds_qc()
                except Exception as e:
                    st.write(f"Error loading Medication QC: {e}")

            # with tab5:
            #     try:
            #         show_microbio_qc()
            #     except Exception as e:
            #         st.write(f"Error loading Microbiology QC: {e}")

            with tab5:
                try:
                    show_patient_qc()
                except Exception as e:
                    st.write(f"Error loading Patient QC: {e}")

            with tab6:
                try:
                    show_patient_assess_qc()
                except Exception as e:
                    st.write(f"Error loading Patient Assessment QC: {e}")

            with tab7:
                try:
                    show_position_qc()
                except Exception as e:
                    st.write(f"Error loading Position QC: {e}")

            with tab8:
                try:
                    show_respiratory_support_qc()
                except Exception as e:
                    st.write(f"Error loading Respiratory Support QC: {e}")

            with tab9:
                try:
                    show_vitals_qc()
                except Exception as e:
                    st.write(f"Error loading Vitals QC: {e}")

  
parent_dir = os.path.dirname(os.path.abspath(__file__))
logo_path = os.path.join(parent_dir, "assets/Picture1.svg")
page = [""]
styles = {
    "nav": {
        "background-color": "#2e3a59",
        "display": "flex",
        "justify-content": "right",  # Center the content horizontally
        "align-items": "right",  # Center the content vertically
        "padding": "10px 0",  # Adjust padding to increase nav bar height
        "height": "100px",
        "font-size": "1.2em",
    },
    "img": {
        "position": "absolute",  # Allow positioning relative to the nav
        "left": "50%",  # Center horizontally
        "top": "50%",   # Center vertically
        "transform": "translate(-50%, -50%)",
        "height": "150px",  # Adjust the logo size to fit the navbar
    },
    "span": {
        "color": "white",
        "font-size": "1.0em",
        "white-space": "nowrap"
    }
}


options = {
    "show_menu": False,
    "show_sidebar": False,
}

selected_page = st_navbar(page, styles=styles, logo_path=logo_path, options=options)
# set_bg_hack_url()
show_home()
