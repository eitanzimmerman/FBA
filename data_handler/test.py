from understat import UnderStat


if __name__ == "__main__":
    understat = UnderStat()
    # players_df = understat.get_players_data(load_exits=False)
    print("*****************************")
    teams_df = understat.get_teams_data(load_exists=False)