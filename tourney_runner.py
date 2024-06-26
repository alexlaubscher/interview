import json, uuid, random, time
from draft import Draft
from player_pool import PlayerPool
from team import Team
import numpy as np
from collections import deque
from supabase import create_client, Client

def main(tourney_name, year):
    url = "https://doexbjnbwrdfoisqigop.supabase.co"
    key = "xyzxyz.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRvZXhiam5id3JkZm9pc3FpZ29wIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTM5NzM4MDEsImV4cCI6MjAwOTU0OTgwMX0.wI83OVGNNAy33FX903iKSyb4fAAPb5xIA0x1SGkQ2KI"
    supabase: Client = create_client(url, key)

    tourney_id = str(uuid.uuid4())
    print('--------')
    print(f'tourney ID: {tourney_id}')
    print('--------')
    table_name = str(year) + '_' + tourney_name

    rules_file_path = "tournaments/" + tourney_name + "/" + tourney_name + ".json"
    with open(rules_file_path, 'r') as rules_file:
        rules = json.load(rules_file)

    with open('scoring/underdog_scoring.json', 'r') as scoring_file:
        scoring = json.load(scoring_file)

    ids, entry_count = generate_entrant_distribution(rules['entrants'])
    drafts = allocate_to_drafts(ids, rules['draft_size'])

    player_pool = PlayerPool(year)

    pg_table = supabase.table('player_game').select('player_id, name, week, fantasy_points').eq('year', str(year)).execute().data
    pg_dict = {(p['name'], p['week']): p for p in pg_table}

    all_rows_to_add = []
    entrants_added = 0
    batch = 1
    start_time = time.time()
    for draft in drafts:
        player_pool.refresh_player_pool()
        teams = []
        for drafter_id in drafts[draft]:
            new_team = Team(drafter_id)
            teams.append(new_team)
        current_draft = Draft(year, teams, rules['rounds'], player_pool)
        current_draft.run_draft()

        teams_as_rows = []
        for team_obj in current_draft.teams:
            row_to_add = get_scores(team_obj.roster, pg_dict, scoring)
            row_to_add['tourney_id'] = tourney_id
            row_to_add['drafter_id'] = team_obj.drafter_id
            row_to_add['entry_id'] = team_obj.entry_id
            row_to_add['regular_season_group'] = current_draft.id
            # Add each one to data_to_add
            teams_as_rows.append(row_to_add)
        
        sorted_teams = sorted(teams_as_rows, key=lambda x: x['regular_season_score'], reverse=True)
        for i, team in enumerate(sorted_teams):
            team['regular_season_ranking'] = i + 1

        all_rows_to_add.extend(teams_as_rows)
        
        if len(all_rows_to_add) % 12000 == 0:
            total_time = time.time() - start_time
            start_time = time.time()
            entrants_added += 12000
            tracker = round(entrants_added / rules['entrants'] * 100, 1)
            print(f'Another 1000 drafts done -- {tracker}% percent done!')
            print(f'Last batch took: {round(total_time, 2)} seconds')

    batch_upload(supabase, all_rows_to_add, table_name)

    # TODO add this when I begin simming for BBM5
    # Assign regular season payouts (if that exists in rules json)

    # TODO this assumes the top 2 teams advance. Is not a safe assumption.
    week_15_teams = supabase.table(table_name).select('*').in_('regular_season_ranking', [1,2]).eq('tourney_id', tourney_id).execute().data
    week_16_teams = run_playoff_week(week_15_teams, 15, rules)
    week_17_teams = run_playoff_week(week_16_teams, 16, rules)
    run_playoff_week(week_17_teams, 17, rules)

    batch_upload(supabase, week_15_teams, table_name)


def generate_entrant_distribution(total_entries=600000):
    entries_remaining = total_entries
    person_ids = []  # Will hold each entry as a person_id
    entry_count = {}  # To hold the number of entries per person

    while entries_remaining > 0:
        if entries_remaining > 150:  
            if np.random.rand() < 0.4:  
                entries = np.random.randint(1, 21)
            elif np.random.rand() > 0.70: 
                entries = np.random.randint(21, 148)
            else:  
                entries = np.random.randint(148, 151)
        else:  
            entries = entries_remaining  

        person_id = str(uuid.uuid4())
        person_ids.extend([person_id] * entries)
        entry_count[person_id] = entries

        entries_remaining -= entries

    random.shuffle(person_ids)
    return person_ids, entry_count


def allocate_to_drafts(ids, num_per_draft=12):
    drafts = {}
    pool = deque(ids)
    current_draft = []

    while pool:
        # Pop the player on the end
        player_to_add = pool.popleft()

        if player_to_add in current_draft and len(pool) > 12:
            pool.append(player_to_add)
            continue
        else:
            current_draft.append(player_to_add)
            if len(current_draft) == num_per_draft:
                draft_id = str(uuid.uuid4())
                drafts[draft_id] = current_draft
                current_draft = []
    return drafts


#TODO this doesn't allow for different scoring systems.
# Please update how this logic works
# It will default to Underdog 0.5 PPR
def get_scores(roster, pg_dict, scoring):
    regular_season_score = 0
    week_15_score = 0
    week_16_score = 0
    week_17_score = 0
    row_for_table = {}
    
    # TODO: Trim down the roster json to make more efficient
    # Fix up the roster json and save it
    row_for_table['roster'] = roster

    for week in range(1, 18):
        weekly_total = 0
        list_of_scores = {'QB': [], 'RB': [], 'WR': [], 'TE': []}

        for player in roster:
            tuple_key = (player['name'], week)
            score = pg_dict.get(tuple_key, {}).get('fantasy_points', 0)
            list_of_scores[player['position']].append(score)
        
        for key in list_of_scores:
            list_of_scores[key].sort()
        
        weekly_total = sum([list_of_scores['QB'].pop() if list_of_scores['QB'] else 0 for _ in range(3)])
        weekly_total += sum([list_of_scores['RB'].pop() if list_of_scores['RB'] else 0 for _ in range(2)])
        weekly_total += sum([list_of_scores['WR'].pop() if list_of_scores['WR'] else 0 for _ in range(3)])
        weekly_total += sum([list_of_scores['TE'].pop() if list_of_scores['TE'] else 0 for _ in range(1)])

        weekly_total += max(
            list_of_scores['RB'].pop() if list_of_scores['RB'] else 0, 
            list_of_scores['WR'].pop() if list_of_scores['WR'] else 0, 
            list_of_scores['TE'].pop() if list_of_scores['TE'] else 0
        )

        if week < 15:
            regular_season_score += weekly_total
        elif week == 15:
            week_15_score += weekly_total
        elif week == 16:
            week_16_score += weekly_total
        elif week == 17:
            week_17_score += weekly_total
        
    row_for_table['regular_season_score'] = round(regular_season_score, 2)
    row_for_table['week_15_score'] = round(week_15_score, 2)
    row_for_table['week_16_score'] = round(week_16_score, 2)
    row_for_table['week_17_score'] = round(week_17_score, 2)

    # Sum weeks 1-14, pull week 15, 16, 17 separately 
    return row_for_table


def get_payout_for_rank(rank, payout_tiers):
    for rank_range, amount in payout_tiers.items():
        if "-" in rank_range:
            min_rank, max_rank = map(int, rank_range.split('-'))
            if min_rank <= rank <= max_rank:
                return amount
        else:
            if rank == int(rank_range):
                return amount
    return 0


def run_playoff_week(playoff_teams, week, rules):
    group_size = rules['Advances'][str(week)][0]
    num_to_adv = rules['Advances'][str(week)][1]
    payout_tiers = rules['tournament payout']

    score_str = 'week_' + str(week) + '_score'
    ranking_str = 'week_' + str(week) + '_ranking' 
    group_str = 'week_' + str(week) + '_group'

    random.shuffle(playoff_teams)
    eliminated_teams = []
    advancing_teams = []

    for i in range(0, len(playoff_teams), group_size):
        group = playoff_teams[i:i+group_size]

        group.sort(key=lambda x: x[score_str], reverse=True)

        group_id = str(uuid.uuid4())

        for rank, team in enumerate(group):
            team[ranking_str] = rank + 1
            team[group_str] = group_id

            if rank + 1 > num_to_adv:
                eliminated_teams.append(team)
            else:
                advancing_teams.append(team)
                
    eliminated_teams.sort(key=lambda x: x[score_str], reverse=True)

    start_placing = len(playoff_teams) - len(eliminated_teams) + 1
    for team in eliminated_teams:
        team['placing'] = start_placing
        payout_amount = get_payout_for_rank(start_placing, payout_tiers)
        team['payout'] = payout_amount
        start_placing += 1

    return advancing_teams


def batch_upload(supabase_client, lst, table_name):
    batch_size = 500
    for i in range(0, len(lst), batch_size):
        batch = lst[i:i+batch_size]   
        supabase_client.table(table_name).upsert(batch).execute()


for year in range(2022, 2023):
    main("the_puppy_4", year)
