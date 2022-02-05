from understat import UnderStat


if __name__ == "__main__":
    understat = UnderStat()
    players_df = understat.get_players_data()
    print(players_df.iloc[1:20, :].T)
    print("*****************************")
    teams_df = understat.get_teams_data()
    print(teams_df.iloc[1:20, :].T)