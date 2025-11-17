import pandas as pd
import numpy as np
import os
from pathlib import Path

if "ETL_FOOTBALL_BASE_DIR" in os.environ:
    BASE_DIR = os.environ["ETL_FOOTBALL_BASE_DIR"]
else:
    BASE_DIR = str(Path(__file__).parent.parent.absolute())

DATA_DIR = os.path.join(BASE_DIR, "data_raw")
DATA_PROCESSED_DIR = os.path.join(BASE_DIR, "data_processed")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)



def save_table(df: pd.DataFrame, filename: str, table_type: str = "table") -> None:
    output_path = os.path.join(DATA_PROCESSED_DIR, filename)
    df.to_csv(output_path, index=False)
    print(f"{table_type} created: {len(df)} records -> {filename}")


def create_dim_player() -> pd.DataFrame:

    print("Creating dim_player:")
    
    # SOURCE 1: Player SEASON stats (primary source with birth year)
    df_season = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_season_stats.csv"),
        header=[0, 1, 2]
    )
    
    player_col = [col for col in df_season.columns if col[0] == 'player' and col[1] == 'Unnamed: 3_level_1'][0]
    pos_col = [col for col in df_season.columns if col[0] == 'pos' and col[1] == 'Unnamed: 5_level_1'][0]
    nation_col = [col for col in df_season.columns if col[0] == 'nation' and col[1] == 'Unnamed: 4_level_1'][0]
    born_col = [col for col in df_season.columns if col[0] == 'born' and col[1] == 'Unnamed: 7_level_1'][0]
    
    df_season_subset = df_season[[player_col, pos_col, nation_col, born_col]].copy()
    df_season_subset.columns = ['player', 'pos', 'nation', 'born']
    
    # SOURCE 2: Player MATCH stats (to catch players missing in season stats)
    df_match = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_match_stats.csv"),
        header=[0, 1, 2]
    )
    
    # Remove header row if exists
    if len(df_match) > 0 and str(df_match.iloc[0, 0]).lower() == 'season':
        df_match = df_match.iloc[1:].reset_index(drop=True)
    
    # Extract player info from match stats
    player_col_match = [col for col in df_match.columns if col[0] == 'player' and col[1] == 'Unnamed: 4_level_1'][0]
    pos_col_match = [col for col in df_match.columns if col[0] == 'pos' and col[1] == 'Unnamed: 7_level_1'][0]
    nation_col_match = [col for col in df_match.columns if col[0] == 'nation' and col[1] == 'Unnamed: 6_level_1'][0]
    
    df_match_subset = df_match[[player_col_match, pos_col_match, nation_col_match]].copy()
    df_match_subset.columns = ['player', 'pos', 'nation']
    df_match_subset['born'] = pd.NA  # Match stats don't have birth year
    
    # COMBINE both sources
    df_combined = pd.concat([df_season_subset, df_match_subset], ignore_index=True)
    
    # Remove duplicates - keep first occurrence (from season stats when available)
    df_combined = df_combined.drop_duplicates(subset='player', keep='first')
    
    # Remove rows with NaN player name
    df_combined = df_combined[df_combined['player'].notna()]
    
    # Sort by player name for consistent ordering
    df_combined = df_combined.sort_values('player').reset_index(drop=True)
    
    # Create player_id sequentially from 1
    df_combined['player_id'] = np.arange(len(df_combined)) + 1
    
    # Convert born to Int64
    df_combined['born'] = pd.to_numeric(df_combined['born'], errors='coerce').astype('Int64')
    
    # Reorder columns with player_id first
    df_combined = df_combined[['player_id', 'player', 'pos', 'nation', 'born']]
    
    # Print summary
    print(f"  -> {len(df_season_subset)} players from season stats")
    print(f"  -> {len(df_match_subset)} players from match stats")
    print(f"  -> {len(df_combined)} unique players total (after deduplication)")
    
    save_table(df_combined, "dim_player.csv", "dim_player")
    return df_combined


def create_dim_team() -> pd.DataFrame:
    print("Creating dim_team:")
    
    # dữ liệu dim_team
    df = pd.read_csv(os.path.join(DATA_DIR, "dim_team.csv"))

    # Loại bỏ dòng header trùng lặp (nếu có)
    header_row = list(df.columns)
    df = df[~df.apply(lambda row: list(row.values) == header_row, axis=1)].reset_index(drop=True)
    
    df_subset = df[['club_id', 'club_label', 'founding_year', 'venue_id']].copy()
    df_subset.columns = ['team_id', 'team_name', 'founded_year', 'stadium_id']
    
    # Map short names
    name_map_short = {
        "AFC Bournemouth": "BOU",
        "Arsenal F.C.": "ARS",
        "Aston Villa F.C.": "AVL",
        "Brentford F.C.": "BRE",
        "Brighton & Hove Albion F.C.": "BHA",
        "Chelsea F.C.": "CHE",
        "Crystal Palace F.C.": "CRY",
        "Everton F.C.": "EVE",
        "Fulham F.C.": "FUL",
        "Ipswich Town F.C.": "IPS",
        "Leicester City F.C.": "LEI",
        "Liverpool F.C.": "LIV",
        "Manchester City F.C.": "MCI",
        "Manchester United F.C.": "MUN",
        "Newcastle United F.C.": "NEW",
        "Nottingham Forest F.C.": "NOT",
        "Southampton F.C.": "SOU",
        "Tottenham Hotspur F.C.": "TOT",
        "West Ham United F.C.": "WHU",
        "Wolverhampton Wanderers F.C.": "WOL",
        "Blackburn Rovers F.C.": "BLA",
        "Bristol City F.C.": "BRC",
        "Burnley F.C.": "BUR",
        "Cardiff City F.C.": "CAR",
        "Coventry City F.C.": "COV",
        "Derby County F.C.": "DER",
        "Hull City A.F.C.": "HUL",
        "Leeds United F.C.": "LEE",
        "Luton Town F.C.": "LUT",
        "Middlesbrough F.C.": "MID",
        "Millwall F.C.": "MIL",
        "Norwich City F.C.": "NOR",
        "Oxford United F.C.": "OXF",
        "Plymouth Argyle F.C.": "PLY",
        "Portsmouth F.C.": "POR",
        "Preston North End F.C.": "PNE",
        "Queens Park Rangers F.C.": "QPR",
        "Sheffield United F.C.": "SHU",
        "Sheffield Wednesday F.C.": "SHW",
        "Stoke City F.C.": "STK",
        "Sunderland A.F.C.": "SUN",
        "Swansea City A.F.C.": "SWA",
        "Watford F.C.": "WAT",
        "West Bromwich Albion F.C.": "WBA"
    }
    
    df_subset["short_name"] = df_subset["team_name"].replace(name_map_short)
    
    # Clean team names - remove F.C., A.F.C., etc.
    remove_words = ["F.C.", "F.C", "FC", "AFC", "A.F.C.", "A.F.C"]
    
    def clean_team_name(name):
        for w in remove_words:
            name = name.replace(w, "")
        return name.strip()
    
    df_subset["team_name"] = df_subset["team_name"].apply(clean_team_name)
    
    # Normalize team names
    name_map = {
        "Brighton & Hove Albion": "Brighton",
        "Manchester United": "Manchester Utd",
        "Newcastle United": "Newcastle Utd",
        "Sheffield United": "Sheffield Utd",
        "Tottenham Hotspur": "Tottenham",
        "West Bromwich Albion": "West Brom",
        "West Ham United": "West Ham",
        "Wolverhampton Wanderers": "Wolves",
        "A Bournemouth": "Bournemouth",
        "Nottingham Forest": "Nott'Ham Forest"
    }
    df_subset["team_name"] = df_subset["team_name"].replace(name_map)
    
    # Loại bỏ "Q" và chuyển sang integer
    df_subset["team_id"] = df_subset["team_id"].astype(str).str.replace("Q", "", regex=False)
    df_subset["team_id"] = pd.to_numeric(df_subset["team_id"], errors='coerce').astype('Int64')
    
    df_subset["stadium_id"] = df_subset["stadium_id"].astype(str).str.replace("Q", "", regex=False)
    df_subset["stadium_id"] = pd.to_numeric(df_subset["stadium_id"], errors='coerce').astype('Int64')
    
    save_table(df_subset, "dim_team.csv", "dim_team")
    return df_subset


def create_dim_stadium() -> pd.DataFrame:
    print("Creating dim_stadium:")
    
    # dữ liệu Dim_stadium
    df = pd.read_csv(os.path.join(DATA_DIR, "dim_team.csv"))
    
    # Loại bỏ dòng header trùng lặp (nếu có)
    header_row = list(df.columns)
    df = df[~df.apply(lambda row: list(row.values) == header_row, axis=1)].reset_index(drop=True)
    
    df_subset = df[['venue_id', 'venue_label', 'capacity']].copy()
    df_subset.columns = ['stadium_id', 'statium_name', 'capacity']
    
    # Loại bỏ dòng có giá trị "capacity" trong cột capacity (nếu còn)
    df_subset = df_subset[df_subset['capacity'].astype(str).str.lower() != 'capacity'].reset_index(drop=True)
    
    # Loại bỏ "Q" và chuyển stadium_id sang integer
    df_subset['stadium_id'] = df_subset['stadium_id'].astype(str).str.replace('Q', '', regex=False)
    df_subset['stadium_id'] = pd.to_numeric(df_subset['stadium_id'], errors='coerce').astype('Int64')
    
    # Chuyển capacity sang integer
    df_subset['capacity'] = pd.to_numeric(df_subset['capacity'], errors='coerce')
    df_subset = df_subset.dropna(subset=['capacity'])
    df_subset['capacity'] = df_subset['capacity'].astype(int)
    
    save_table(df_subset, "dim_stadium.csv", "dim_stadium")
    return df_subset


def create_dim_match() -> pd.DataFrame:
    print("Creating dim_match:")
    
    df_raw = pd.read_csv(os.path.join(DATA_DIR, "fbref_fact_team_match.csv"))
    
    # remove duplicates
    unique_matches = df_raw.drop_duplicates(subset=['game']).copy()
    print(f"  -> After removing duplicates: {len(unique_matches)} unique games")
    
    # Create index
    unique_matches = unique_matches.reset_index(drop=True)
    unique_matches['game_id'] = range(1, len(unique_matches) + 1)
    
    # convert data types
    df_subset = unique_matches[['game_id', 'game', 'date', 'round', 'day']].copy()
    df_subset['game'] = df_subset['game'].astype(str).str.strip()
    
    # Clean date format 
    df_subset['date'] = df_subset['date'].astype(str).str.split(' ').str[0]
    df_subset['date'] = pd.to_datetime(df_subset['date'], errors='coerce')
    
    df_subset['round'] = df_subset['round'].astype(str).str.strip()
    df_subset['day'] = df_subset['day'].astype(str).str.strip()
    
    # remove missing date after datetime conversion
    before_count = len(df_subset)
    df_subset = df_subset.dropna(subset=['date']).copy()
    removed_count = before_count - len(df_subset)
    print(f"  -> Removed {removed_count} rows with invalid/missing dates")
    
    # Recreate game_id for remaining rows
    df_subset = df_subset.reset_index(drop=True)
    df_subset['game_id'] = range(1, len(df_subset) + 1)
    df_subset['game_id'] = df_subset['game_id'].astype('Int64')
    
    print(f"  -> Final: {len(df_subset)} games with valid dates")
    
    save_table(df_subset, "dim_match.csv", "dim_match")
    return df_subset



def create_fact_team_match() -> pd.DataFrame:
    """Create fact table for team match statistics."""
    print("Creating fact_team_match_clean:")
    
    df = pd.read_csv(os.path.join(DATA_DIR, "fbref_fact_team_match.csv"))
    # Chỉ dropna cho các cột bắt buộc, giữ lại các trận chưa đá (có thể có NaN ở GF, GA, etc.)
    df = df.dropna(subset=['team', 'opponent', 'game'])
    
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    df_match = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
    df_player = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
    
    # CHUẨN HÓA CHUỖI (rất quan trọng)
    df['team'] = df['team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    df['game'] = df['game'].astype(str).str.strip().str.lower()
    df_match['game'] = df_match['game'].astype(str).str.strip().str.lower()
    
    df['Captain'] = df['Captain'].astype(str).str.strip().str.lower()
    df_player['player'] = df_player['player'].astype(str).str.strip().str.lower()
    
    df['opponent'] = df['opponent'].astype(str).str.strip().str.lower()
    
    # Normalize team names to match dim_team (before merge)
    name_map = {
        "brighton & hove albion": "brighton",
        "manchester united": "manchester utd",
        "newcastle united": "newcastle utd",
        "sheffield united": "sheffield utd",
        "tottenham hotspur": "tottenham",
        "west bromwich albion": "west brom",
        "west ham united": "west ham",
        "wolverhampton wanderers": "wolves",
        "nottingham forest": "nott'ham forest",
        "sunderland": "sunderland a.",  # Map "Sunderland" to "Sunderland A." to match dim_team
        "swansea city": "swansea city a.",  # Map "Swansea City" to "Swansea City A." to match dim_team
        "hull city": "hull city a."  # Map "Hull City" to "Hull City A." to match dim_team
    }
    df['team'] = df['team'].replace(name_map)
    df['opponent'] = df['opponent'].replace(name_map)
    
    # Apply clean function giống như trong create_dim_team
    remove_words = ["f.c.", "f.c", "fc", "afc", "a.f.c.", "a.f.c"]
    def clean_team_name(name):
        # name đã lowercase rồi, chỉ cần remove words
        for w in remove_words:
            name = name.replace(w, "")
        return name.strip()
    
    df['team'] = df['team'].apply(clean_team_name)
    df['opponent'] = df['opponent'].apply(clean_team_name)

    # Map Captain → captain_id
    df = df.merge(
        df_player[['player_id', 'player']],
        left_on='Captain',
        right_on='player',
        how='left'
    )
    df.rename(columns={'player_id': 'captain_id'}, inplace=True)
    df.drop(columns=['player'], inplace=True)
    
    # MAP TEAM → team_id
    df = df.merge(
        df_team[['team_id', 'team_name']].rename(columns={'team_name': 'team'}),
        on='team',
        how='left'
    )
    
    # MAP OPPONENT → opponent_id
    df = df.merge(
        df_team[['team_id', 'team_name']].rename(columns={'team_name': 'opponent'}),
        on='opponent',
        how='left',
        suffixes=('', '_opp')
    )
    
    df.rename(columns={'team_id_opp': 'opponent_id'}, inplace=True)
    
    # MAP GAME → game_id
    df = df.merge(
        df_match[['game_id', 'game']],
        on='game',
        how='left'
    )
    

    # Loại bỏ "Q" và chuyển team_id và opponent_id sang integer
    if 'team_id' in df.columns:
        df['team_id'] = df['team_id'].astype(str).str.replace('Q', '', regex=False)
        df['team_id'] = pd.to_numeric(df['team_id'], errors='coerce').astype('Int64')
    if 'opponent_id' in df.columns:
        df['opponent_id'] = df['opponent_id'].astype(str).str.replace('Q', '', regex=False)
        df['opponent_id'] = pd.to_numeric(df['opponent_id'], errors='coerce').astype('Int64')
    
    # CHUẨN HÓA CỘT round
    df["round"] = df["round"].apply(lambda x: x.split()[-1].zfill(2))
    
    # TẠO SUBSET CỘT FACT CUỐI
    df_subset = df[[
        'season',
        'game_id',
        'team_id',
        'opponent_id',
        'round',
        'venue',
        'result',
        'GF',
        'GA',
        'xG',
        'xGA',
        'Poss',
        'captain_id',
        'Formation',
        'Opp Formation',
    ]]
    df_subset = df_subset.dropna(subset=['result'])
    save_table(df_subset, 'fact_team_match_clean.csv', "fact_team_match_clean")
    return df_subset


def create_fact_player_match() -> pd.DataFrame:
    print("Creating fact_player_match_clean:")
    
    # dữ liệu fact_playermatchstats
    df = pd.read_csv(
        os.path.join(DATA_DIR, "fbref_fact_player_match_stats.csv"),
        header=[0, 1, 2]
    )
    
    df_match = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_match.csv'))
    df_player = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_player.csv'))
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    
    # Xác định tên cột dựa trên cấu trúc thực tế (match 2 level đầu)
    season_col_name = [col for col in df.columns if col[0] == 'season' and col[1] == 'Unnamed: 1_level_1'][0]
    game_col_name = [col for col in df.columns if col[0] == 'game' and col[1] == 'Unnamed: 2_level_1'][0]
    team_col_name = [col for col in df.columns if col[0] == 'team' and col[1] == 'Unnamed: 3_level_1'][0]
    player_col_name = [col for col in df.columns if col[0] == 'player' and col[1] == 'Unnamed: 4_level_1'][0]
    
    # Loại bỏ dòng đầu tiên nếu có giá trị "season" (dòng header thật trong file CSV)
    if len(df) > 0 and str(df.iloc[0][season_col_name]).lower() == 'season':
        df = df.iloc[1:].reset_index(drop=True)
        print(f"Đã loại bỏ dòng header trùng lặp. Số dòng còn lại: {len(df)}")
    
    # Tìm các cột khác bằng cách match 2 level đầu
    min_col = [col for col in df.columns if col[0] == 'min' and col[1] == 'Unnamed: 9_level_1'][0]
    gls_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Gls'][0]
    ast_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Ast'][0]
    pk_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'PK'][0]
    pkatt_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'PKatt'][0]
    sh_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Sh'][0]
    sot_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'SoT'][0]
    crdy_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'CrdY'][0]
    crdr_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'CrdR'][0]
    touches_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Touches'][0]
    tkl_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Tkl'][0]
    int_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Int'][0]
    blocks_col = [col for col in df.columns if col[0] == 'Performance' and col[1] == 'Blocks'][0]
    # Expected stats: xG và xAG (expected assists, còn gọi là xA)
    xg_col = [col for col in df.columns if col[0] == 'Expected' and col[1] == 'xG'][0]
    xag_col = [col for col in df.columns if col[0] == 'Expected' and col[1] == 'xAG'][0]
    sca_col = [col for col in df.columns if col[0] == 'SCA' and col[1] == 'SCA'][0]
    gca_col = [col for col in df.columns if col[0] == 'SCA' and col[1] == 'GCA'][0]
    cmp_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'Cmp'][0]
    att_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'Att'][0]
    cmppct_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'Cmp%'][0]
    prgp_col = [col for col in df.columns if col[0] == 'Passes' and col[1] == 'PrgP'][0]
    carries_col = [col for col in df.columns if col[0] == 'Carries' and col[1] == 'Carries'][0]
    prgc_col = [col for col in df.columns if col[0] == 'Carries' and col[1] == 'PrgC'][0]
    to_att_col = [col for col in df.columns if col[0] == 'Take-Ons' and col[1] == 'Att'][0]
    to_succ_col = [col for col in df.columns if col[0] == 'Take-Ons' and col[1] == 'Succ'][0]
    
    df_subset = df[[season_col_name, game_col_name, team_col_name, player_col_name,
                    min_col, gls_col, xg_col, xag_col, ast_col, pk_col, pkatt_col, sh_col, sot_col,
                    crdy_col, crdr_col, touches_col, tkl_col, int_col, blocks_col,
                    sca_col, gca_col, cmp_col, att_col, cmppct_col, prgp_col,
                    carries_col, prgc_col, to_att_col, to_succ_col]].copy()
    
    df_subset.columns = [
        'season', 'game', 'team', 'player', 'min_played', 'goals', 'xG', 'xA',
        'assists', 'penalty_made', 'penalty_attempted', 'shots', 'shots_on_target',
        'yellow_cards', 'red_cards', 'touches', 'tackles', 'interceptions',
        'blocks', 'shot_creating_actions', 'goal_creating_actions',
        'passes_completed', 'passes_attempted', 'pass_completion_percent',
        'progressive_passes', 'carries', 'progressive_carries',
        'take_ons_attempted', 'take_ons_successful'
    ]
    
    # Normalize team names to match dim_team (before merge)
    name_map = {
        "Brighton & Hove Albion": "Brighton",
        "Manchester United": "Manchester utd",
        "Newcastle United": "Newcastle utd",
        "Sheffield United": "Sheffield utd",
        "Tottenham Hotspur": "Tottenham",
        "West Bromwich Albion": "West brom",
        "West Ham United": "West ham",
        "Wolverhampton Wanderers": "Wolves",
        "Nottingham Forest": "Nott'ham forest",
        "Sunderland": "Sunderland A.",  # Map "Sunderland" to "Sunderland A." to match dim_team
        "Swansea City": "Swansea City A.",
        "Hull City": "Hull City A."
    }
    df_subset["team"] = df_subset["team"].replace(name_map)
    
    # Chuẩn hóa chuỗi cho game
    df_subset['game'] = df_subset['game'].astype(str).str.strip().str.lower()
    df_match['game'] = df_match['game'].astype(str).str.strip().str.lower()
    
    # Map game → game_id
    df_subset = df_subset.merge(
        df_match[['game_id', 'game']],
        on='game',
        how='left'
    )
    
    # Chuẩn hóa cho team
    df_subset['team'] = df_subset['team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    # Apply clean function giống như trong create_dim_team
    remove_words = ["f.c.", "f.c", "fc", "afc", "a.f.c.", "a.f.c"]
    def clean_team_name(name):
        name_lower = name.lower()
        for w in remove_words:
            name_lower = name_lower.replace(w, "")
        return name_lower.strip()
    
    df_subset['team'] = df_subset['team'].apply(clean_team_name)
    
    # Map team → team_id
    df_subset = df_subset.merge(
        df_team[['team_id', 'team_name']],
        left_on='team',
        right_on='team_name',
        how='left'
    )
    
    # Filter out rows with NULL team_id (unmatched team names)
    initial_count = len(df_subset)
    null_team = df_subset[df_subset['team_id'].isna()]
    
    if len(null_team) > 0:
        unmatched_teams = null_team['team'].unique()
        print(f"  Warning: {len(null_team)} rows with unmatched team names: {list(unmatched_teams)[:5]}")
    
    df_subset = df_subset.dropna(subset=['team_id'])
    filtered_count = initial_count - len(df_subset)
    if filtered_count > 0:
        print(f"  -> Filtered out {filtered_count} rows with NULL team_id")
    
    # Xóa cột thừa
    if 'team_name' in df_subset.columns:
        df_subset.drop(columns=['team_name'], inplace=True)
    
    # Đảm bảo team_id là integer (loại bỏ "Q" nếu có)
    if 'team_id' in df_subset.columns:
        df_subset['team_id'] = df_subset['team_id'].astype(str).str.replace('Q', '', regex=False)
        df_subset['team_id'] = pd.to_numeric(df_subset['team_id'], errors='coerce').astype('Int64')
    
    # Filter again after conversion
    df_subset = df_subset.dropna(subset=['team_id'])
    
    # Chuẩn hóa cho player
    df_subset['player'] = df_subset['player'].astype(str).str.strip().str.lower()
    df_player['player'] = df_player['player'].astype(str).str.strip().str.lower()
    
    # Map player → player_id
    df_subset = df_subset.merge(
        df_player[['player_id', 'player']],
        on='player',
        how='left'
    )
    
    # Filter out rows with NULL player_id or game_id
    initial_count = len(df_subset)
    null_player = df_subset[df_subset['player_id'].isna()]
    null_game = df_subset[df_subset['game_id'].isna()]
    
    if len(null_player) > 0:
        print(f"  Warning: {len(null_player)} rows with unmatched player names")
    if len(null_game) > 0:
        print(f"  Warning: {len(null_game)} rows with unmatched game names")
    
    df_subset = df_subset.dropna(subset=['player_id', 'game_id'])
    filtered_count = initial_count - len(df_subset)
    if filtered_count > 0:
        print(f"  -> Filtered out {filtered_count} rows with NULL player_id or game_id")
    
    df_subset = df_subset[['season', 'game_id', 'team_id', 'player_id',
                           'min_played', 'goals', 'xG', 'xA', 'assists', 'penalty_made',
                           'penalty_attempted', 'shots', 'shots_on_target',
                           'yellow_cards', 'red_cards', 'touches', 'tackles',
                           'interceptions', 'blocks', 'shot_creating_actions',
                           'goal_creating_actions', 'passes_completed',
                           'passes_attempted', 'pass_completion_percent',
                           'progressive_passes', 'carries', 'progressive_carries',
                           'take_ons_attempted', 'take_ons_successful']]
    
    save_table(df_subset, 'fact_player_match_clean.csv', "fact_player_match_clean")
    return df_subset


def create_fact_team_point() -> pd.DataFrame:
    print("Creating fact_team_point:")
    
    if os.path.exists(os.path.join(DATA_DIR, 'team_point.csv')):
        df = pd.read_csv(os.path.join(DATA_DIR, 'team_point.csv'))
    else:
        print("  -> team_point.csv not found, skipping")
        return pd.DataFrame()
    
    df_team = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, 'dim_team.csv'))
    
    def convert_season(season):
        # season dạng "2024/2025"
        season_str = str(season)
        if "/" in season_str:
            parts = season_str.split("/")
            if len(parts) == 2:
                y1 = parts[0][-2:]   # lấy 2 số cuối của năm đầu
                y2 = parts[1][-2:]   # lấy 2 số cuối của năm sau
                return int(y1 + y2)  # ghép lại thành 2425 (integer)
        return season
    
    if "Mùa giải" in df.columns:
        df["Mùa giải"] = df["Mùa giải"].apply(convert_season)
        df = df.rename(columns={"Mùa giải": "season_id"})
    elif "season_id" in df.columns:
        # Nếu đã có season_id rồi, chỉ cần convert nếu chưa convert
        df["season_id"] = df["season_id"].apply(convert_season)
    
    # tên khác form - normalize team names to match dim_team
    name_map = {
        "Ipswich": "Ipswich Town",
        "Luton": "Luton Town",
        "Newcastle": "Newcastle utd",
        "Leeds": "Leeds United",
        "Leicester": "Leicester City",
        "Norwich": "Norwich City",
        "Nottingham": "Nott'ham forest",
        "Sunderland": "Sunderland A.",  # Map "Sunderland" to "Sunderland A." to match dim_team
        "Swansea City": "Swansea City A.",
        "Hull City": "Hull City A."
    }
    # Thay thế tên đội
    df["Team"] = df["Team"].replace(name_map)
    
    # Chuẩn hóa cho team
    df['Team'] = df['Team'].astype(str).str.strip().str.lower()
    df_team['team_name'] = df_team['team_name'].astype(str).str.strip().str.lower()
    
    # Apply clean function giống như trong create_dim_team
    remove_words = ["f.c.", "f.c", "fc", "afc", "a.f.c.", "a.f.c"]
    def clean_team_name(name):
        name_lower = name.lower()
        for w in remove_words:
            name_lower = name_lower.replace(w, "")
        return name_lower.strip()
    
    df['Team'] = df['Team'].apply(clean_team_name)
    
    # Map team → team_id
    df = df.merge(
        df_team[['team_id', 'team_name']],
        left_on='Team',
        right_on='team_name',
        how='left'
    )
    
    # Filter out rows with NULL team_id (unmatched team names)
    initial_count = len(df)
    null_team = df[df['team_id'].isna()]
    
    if len(null_team) > 0:
        unmatched_teams = null_team['Team'].unique()
        print(f"  Warning: {len(null_team)} rows with unmatched team names: {list(unmatched_teams)[:5]}")
    
    df = df.dropna(subset=['team_id'])
    filtered_count = initial_count - len(df)
    if filtered_count > 0:
        print(f"  -> Filtered out {filtered_count} rows with NULL team_id")
    
    # Xóa cột thừa
    if 'team_name' in df.columns:
        df.drop(columns=['team_name'], inplace=True)
    
    # đổi kiểu dữ liệu cột rank
    df["Rank"] = df["Rank"].astype(int)
    
    # Tách GF và GA từ cột "GF:GA"
    df[["GF", "GA"]] = df["GF:GA"].str.split(":", expand=True)
    
    # Chuyển kiểu dữ liệu sang int
    df["GF"] = df["GF"].astype(int)
    df["GA"] = df["GA"].astype(int)
    
    # Xóa cột cũ
    df.drop(columns=["GF:GA"], inplace=True)
    
    df_subset = df[["season_id", "Match_Category", "Rank", "team_id",
                    "MP", "W", "D", "L", "GF", "GA", "GD", "Pts", "Recent_Form"]]
    
    save_table(df_subset, 'fact_team_point.csv', "fact_team_point")
    return df_subset


if __name__ == "__main__":
    print("ETL Football - Transform Process")
    
    # Create dim phai chay truoc fact
    create_dim_player()
    create_dim_team()
    create_dim_stadium()
    create_dim_match()
    
    # Create fact phai chay sau dim
    create_fact_team_match()
    create_fact_player_match()
    create_fact_team_point()
    
    print("Transform process completed!")