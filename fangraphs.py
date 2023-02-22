import pandas_gbq
import argparse
from pybaseball import batting_stats, pitching_stats
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    'bigquery_credentials.json'
)
project_id = 'baseball-source'

parser = argparse.ArgumentParser(description="Load Fangraphs data into Google BigQuery")
parser.add_argument('-s', '--start', type=int, default=1871, help="first year to extract from Retrosheet")
parser.add_argument('-e', '--end', type=int, default=2022, help="last year to extract from Retrosheet")

args = parser.parse_args()
start_year = args.start
end_year = args.end + 1

renamed_columns = {
    '1B': '_1B',
    '2B': '_2B',
    '3B': '_3B',
    'BB%': 'BB_pct',
    'K%': 'K_pct',
    'BB/K': 'BB_K',
    'GB/FB': 'GB_FB',
    'LD%': 'LD_pct',
    'GB%': 'GB_pct',
    'FB%': 'FB_pct',
    'IFFB%': 'IFFB_pct',
    'HR/FB': 'HR_FB',
    'IFH%': 'IFH_pct',
    'BUH%': 'BUH_pct',
    'wRC+': 'wRC_plus',
    '-WPA': 'minus_WPA',
    '+WPA': 'plus_WPA',
    'WPA/LI': 'WPA_LI',
    'FB% (Pitch)': 'FB_pct_pitch',
    'SL%': 'SL_pct',
    'CT%': 'CT_pct',
    'CB%': 'CB_pct',
    'CH%': 'CH_pct',
    'SF%': 'SF_pct',
    'KN%': 'KN_pct',
    'XX%': 'XX_pct',
    'PO%': 'PO_pct',
    'wFB/C': 'wFB_C',
    'wSL/C': 'wSL_C',
    'wCT/C': 'wCT_C',
    'wCB/C': 'wCB_C',
    'wCH/C': 'wCH_C',
    'wSF/C': 'wSF_C',
    'wKN/C': 'wKN_C',
    'O-Swing%': 'O_Swing_pct',
    'Z-Swing%': 'Z_Swing_pct',
    'Swing%': 'Swing_pct',
    'O-Contact%': 'O_Contact_pct',
    'Z-Contact%': 'Z_Contact_pct',
    'Contact%': 'Contact_pct',
    'Zone%': 'Zone_pct',
    'F-Strike%': 'F_Strike_pct',
    'SwStr%': 'SwStr_pct',
    'FA% (sc)': 'FA_pct_sc',
    'FT% (sc)': 'FT_pct_sc',
    'FC% (sc)': 'FC_pct_sc',
    'FS% (sc)': 'FS_pct_sc',
    'FO% (sc)': 'FO_pct_sc',
    'SI% (sc)': 'SI_pct_sc',
    'SL% (sc)': 'SL_pct_sc',
    'CU% (sc)': 'CU_pct_sc',
    'KC% (sc)': 'KC_pct_sc',
    'EP% (sc)': 'EP_pct_sc',
    'CH% (sc)': 'CH_pct_sc',
    'SC% (sc)': 'SC_pct_sc',
    'KN% (sc)': 'KN_pct_sc',
    'UN% (sc)': 'UN_pct_sc',
    'vFA (sc)': 'vFA_sc',
    'vFT (sc)': 'vFT_sc',
    'vFC (sc)': 'vFC_sc',
    'vFS (sc)': 'vFS_sc',
    'vFO (sc)': 'vFO_sc',
    'vSI (sc)': 'vSI_sc',
    'vSL (sc)': 'vSL_sc',
    'vCU (sc)': 'vCU_sc',
    'vKC (sc)': 'vKC_sc',
    'vEP (sc)': 'vEP_sc',
    'vCH (sc)': 'vCH_sc',
    'vSC (sc)': 'vSC_sc',
    'vKN (sc)': 'vKN_sc',
    'FA-X (sc)': 'FA_X_sc',
    'FT-X (sc)': 'FT_X_sc',
    'FC-X (sc)': 'FC_X_sc',
    'FS-X (sc)': 'FS_X_sc',
    'FO-X (sc)': 'FO_X_sc',
    'SI-X (sc)': 'SI_X_sc',
    'SL-X (sc)': 'SL_X_sc',
    'CU-X (sc)': 'CU_X_sc',
    'KC-X (sc)': 'KC_X_sc',
    'EP-X (sc)': 'EP_X_sc',
    'CH-X (sc)': 'CH_X_sc',
    'SC-X (sc)': 'SC_X_sc',
    'KN-X (sc)': 'KN_X_sc',
    'FA-Z (sc)': 'FA_Z_sc',
    'FT-Z (sc)': 'FT_Z_sc',
    'FC-Z (sc)': 'FC_Z_sc',
    'FS-Z (sc)': 'FS_Z_sc',
    'FO-Z (sc)': 'FO_Z_sc',
    'SI-Z (sc)': 'SI_Z_sc',
    'SL-Z (sc)': 'SL_Z_sc',
    'CU-Z (sc)': 'CU_Z_sc',
    'KC-Z (sc)': 'KC_Z_sc',
    'EP-Z (sc)': 'EP_Z_sc',
    'CH-Z (sc)': 'CH_Z_sc',
    'SC-Z (sc)': 'SC_Z_sc',
    'KN-Z (sc)': 'KN_Z_sc',
    'wFA (sc)': 'wFA_sc',
    'wFT (sc)': 'wFT_sc',
    'wFC (sc)': 'wFC_sc',
    'wFS (sc)': 'wFS_sc',
    'wFO (sc)': 'wFO_sc',
    'wSI (sc)': 'wSI_sc',
    'wSL (sc)': 'wSL_sc',
    'wCU (sc)': 'wCU_sc',
    'wKC (sc)': 'wKC_sc',
    'wEP (sc)': 'wEP_sc',
    'wCH (sc)': 'wCH_sc',
    'wSC (sc)': 'wSC_sc',
    'wKN (sc)': 'wKN_sc',
    'wFA/C (sc)': 'wFA_C_sc',
    'wFT/C (sc)': 'wFT_C_sc',
    'wFC/C (sc)': 'wFC_C_sc',
    'wFS/C (sc)': 'wFS_C_sc',
    'wFO/C (sc)': 'wFO_C_sc',
    'wSI/C (sc)': 'wSI_C_sc',
    'wSL/C (sc)': 'wSL_C_sc',
    'wCU/C (sc)': 'wCU_C_sc',
    'wKC/C (sc)': 'wKC_C_sc',
    'wEP/C (sc)': 'wEP_C_sc',
    'wCH/C (sc)': 'wCH_C_sc',
    'wSC/C (sc)': 'wSC_C_sc',
    'wKN/C (sc)': 'wKN_C_sc',
    'O-Swing% (sc)': 'O_Swing_pct_sc',
    'O-Swing% (sc)1': 'O_Swing_pct_sc',
    'Z-Swing% (sc)': 'Z_Swing_pct_sc',
    'Swing% (sc)': 'Swing_pct_sc',
    'O-Contact% (sc)': 'O_Contact_pct_sc',
    'Z-Contact% (sc)': 'Z_Contact_pct_sc',
    'Contact% (sc)': 'Contact_pct_sc',
    'Zone% (sc)': 'Zone_pct_sc',
    'Pull%': 'Pull_pct',
    'Cent%': 'Cent_pct',
    'Oppo%': 'Oppo_pct',
    'Soft%': 'Soft_pct',
    'Med%': 'Med_pct',
    'Hard%': 'Hard_pct',
    'TTO%': 'TTO_pct',
    'CH% (pi)': 'CH_pct_pi',
    'CS% (pi)': 'CS_pct_pi',
    'CU% (pi)': 'CU_pct_pi',
    'FA% (pi)': 'FA_pct_pi',
    'FC% (pi)': 'FC_pct_pi',
    'FS% (pi)': 'FS_pct_pi',
    'KN% (pi)': 'KN_pct_pi',
    'SB% (pi)': 'SB_pct_pi',
    'SI% (pi)': 'SI_pct_pi',
    'SL% (pi)': 'SL_pct_pi',
    'XX% (pi)': 'XX_pct_pi',
    'vCH (pi)': 'vCH_pi',
    'vCS (pi)': 'vCS_pi',
    'vCU (pi)': 'vCU_pi',
    'vFA (pi)': 'vFA_pi',
    'vFC (pi)': 'vFC_pi',
    'vFS (pi)': 'vFS_pi',
    'vKN (pi)': 'vKN_pi',
    'vSB (pi)': 'vSB_pi',
    'vSI (pi)': 'vSI_pi',
    'vSL (pi)': 'vSL_pi',
    'vXX (pi)': 'vXX_pi',
    'CH-X (pi)': 'CH_X_pi',
    'CS-X (pi)': 'CS_X_pi',
    'CU-X (pi)': 'CU_X_pi',
    'FA-X (pi)': 'FA_X_pi',
    'FC-X (pi)': 'FC_X_pi',
    'FS-X (pi)': 'FS_X_pi',
    'KN-X (pi)': 'KN_X_pi',
    'SB-X (pi)': 'SB_X_pi',
    'SI-X (pi)': 'SI_X_pi',
    'SL-X (pi)': 'SL_X_pi',
    'XX-X (pi)': 'XX_X_pi',
    'CH-Z (pi)': 'CH_Z_pi',
    'CS-Z (pi)': 'CS_Z_pi',
    'CU-Z (pi)': 'CU_Z_pi',
    'FA-Z (pi)': 'FA_Z_pi',
    'FC-Z (pi)': 'FC_Z_pi',
    'FS-Z (pi)': 'FS_Z_pi',
    'KN-Z (pi)': 'KN_Z_pi',
    'SB-Z (pi)': 'SB_Z_pi',
    'SI-Z (pi)': 'SI_Z_pi',
    'SL-Z (pi)': 'SL_Z_pi',
    'XX-Z (pi)': 'XX_Z_pi',
    'wCH (pi)': 'wCH_pi',
    'wCS (pi)': 'wCS_pi',
    'wCU (pi)': 'wCU_pi',
    'wFA (pi)': 'wFA_pi',
    'wFC (pi)': 'wFC_pi',
    'wFS (pi)': 'wFS_pi',
    'wKN (pi)': 'wKN_pi',
    'wSB (pi)': 'wSB_pi',
    'wSI (pi)': 'wSI_pi',
    'wSL (pi)': 'wSL_pi',
    'wXX (pi)': 'wXX_pi',
    'wCH/C (pi)': 'wCH_C_pi',
    'wCS/C (pi)': 'wCS_C_pi',
    'wCU/C (pi)': 'wCU_C_pi',
    'wFA/C (pi)': 'wFA_C_pi',
    'wFC/C (pi)': 'wFC_C_pi',
    'wFS/C (pi)': 'wFS_C_pi',
    'wKN/C (pi)': 'wKN_C_pi',
    'wSB/C (pi)': 'wSB_C_pi',
    'wSI/C (pi)': 'wSI_C_pi',
    'wSL/C (pi)': 'wSL_C_pi',
    'wXX/C (pi)': 'wXX_C_pi',
    'O-Swing% (pi)': 'O_Swing_pct_pi',
    'Z-Swing% (pi)': 'Z_Swing_pct_pi',
    'Swing% (pi)': 'Swing_pct_pi',
    'O-Contact% (pi)': 'O_Contact_pct_pi',
    'Z-Contact% (pi)': 'Z_Contact_pct_pi',
    'Contact% (pi)': 'Contact_pct_pi',
    'Zone% (pi)': 'Zone_pct_pi',
    'Pace (pi)': 'Pace_pi',
    'AVG+': 'AVG_plus',
    'BB%+': 'BB_pct_plus',
    'K%+': 'K_pct_plus',
    'OBP+': 'OBP_plus',
    'SLG+': 'SLG_plus',
    'ISO+': 'ISO_plus',
    'BABIP+': 'BABIP_plus',
    'LD+%': 'LD_pct_plus',
    'LD%+': 'LD_pct_plus',
    'GB%+': 'GB_pct_plus',
    'FB%+': 'FB_pct_plus',
    'HR/FB%+': 'HR_FB_pct_plus',
    'Pull%+': 'Pull_pct_plus',
    'Cent%+': 'Cent_pct_plus',
    'Oppo%+': 'Oppo_pct_plus',
    'Soft%+': 'Soft_pct_plus',
    'Med%+': 'Med_pct_plus',
    'Hard%+': 'Hard_pct_plus',
    'Barrel%': 'Barrel_pct',
    'HardHit%': 'HardHit_pct',
    'K/9': 'K_9',
    'BB/9': 'BB_9',
    'K/BB': 'K_BB',
    'H/9': 'H_9',
    'HR/9': 'HR_9',
    'LOB%': 'LOB_pct',
    'Start-IP': 'Start_IP',
    'Relief-IP': 'Relief_IP',
    'FB% 2': 'FB_pct_2',
    'ERA-': 'ERA_minus',
    'FIP-': 'FIP_minus',
    'xFIP-': 'xFIP_minus',
    'RS/9': 'RS_9',
    'E-F': 'E_F',
    'RA9-WAR': 'RA9_WAR',
    'BIP-Wins': 'BIP_Wins',
    'LOB-Wins': 'LOB_Wins',
    'FDP-Wins': 'FDP_Wins',
    'Age Rng': 'Age_Rng',
    'K-BB%': 'K_BB_pct',
    'K/9+': 'K_9_plus',
    'BB/9+': 'BB_9_plus',
    'H/9+': 'H_9_plus',
    'HR/9+': 'HR_9_plus',
    'LOB%+': 'LOB_pct_plus',
    'WHIP+': 'WHIP_plus',
    'CStr%': 'CStr_pct',
    'CSW%': 'CSW_pct',
    'K/BB+': 'K_BB_plus',
    'L-WAR': 'Legacy_WAR'
}


def fangraphs(start_year, end_year):
    years = range(start_year, end_year)
    for year in years:
        for side in ['batting', 'pitching']:
            print('Loading Fangraphs ' + str(year) + ' ' + side)
            table_name = side + '_' + str(year)
            table_id = "fangraphs." + table_name
            if side == 'batting':
                df = batting_stats(year, qual=0)
            elif side == 'pitching':
                df = pitching_stats(year, qual=0)
            df.rename(columns=renamed_columns, inplace=True)
            table_schema = []
            for column in df.columns:
                table_schema.append({'name': column, 'type': 'STRING'})
            pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace', table_schema=table_schema, api_method='load_csv', chunksize=10000)

fangraphs(start_year=start_year, end_year=end_year)