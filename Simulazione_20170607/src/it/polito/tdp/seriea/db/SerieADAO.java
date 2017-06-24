package it.polito.tdp.seriea.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.seriea.model.Match;
import it.polito.tdp.seriea.model.Season;
import it.polito.tdp.seriea.model.Team;

public class SerieADAO {

	// GET SEMPLICI
	private static final String GET_ALL_MATCHES = "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches";

	// GET CON FILTRO STAGIONE
	private static final String GET_MATCHES_BY_SEASON = "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where Season = ?";
	private static final String GET_MATCHES_FROM_SEASON = "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where Season >= ?";
	private static final String GET_MATCHES_TO_SEASON = " select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where Season <= ?";

	// STATISTICHE TOTALI
	private static final String NUMERO_VITTORIE_A_SU_B = "select count(*) as vittorieTotali from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG)";
	private static final String NUMERO_VITTORIE_A_SU_B_IN_CASA = "select count(*) as vittorieInCasa from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG";
	private static final String NUMERO_VITTORIE_A_SU_FUORI_CASA = "select count(*) as vittorieFuoriCasa from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG";
	private static final String VITTORIE_A_SU_B= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG)";
	private static final String VITTORIE_A_SU_B_IN_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG";
	private static final String VITTORIE_A_SU_B_FUORI_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG";
	
	private static final String GOAL_TOTALI_PRIMO_TEMPO = "select((select sum(HTHG) from matches where HomeTeam = A) + (select sum(HTAG) from matches where AwayTeam = A)) as goalTotaliPrimoTempo";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_IN_CASA = "select sum(HTHG) from matches where HomeTeam = A";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_FUORI_CASA = "select sum(HTAG) from matches where AwayTeam = A";
	
	// STATISTICHE PER UNA PARTICOLARE STAGIONE
	private static final String NUMERO_VITTORIE_A_SU_B_BY_SEASON= "select count(*) as vittorieTotali from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG) and Season = ?";
	private static final String NUMERO_VITTORIE_A_SU_B_BY_SEASON_IN_CASA= "select count(*) as vittorieInCasa from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG and Season = ?";
	private static final String NUMERO_VITTORIE_A_SU_B_BY_SEASON_FUORI_CASA= "select count(*) as vittorieFuoriCasa from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG and Season = ?";
	private static final String VITTORIE_A_SU_B_BY_SEASON= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG) and Season = ?";
	private static final String VITTORIE_A_SU_B_BY_SEASON_IN_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG and Season = ?";
	private static final String VITTORIE_A_SU_B_BY_SEASON_FUORI_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG and Season = ?";
	
	private static final String GOAL_TOTALI_PRIMO_TEMPO_BY_SEASON = "select((select sum(HTHG) from matches where HomeTeam = A and Season = ?) + (select sum(HTAG) from matches where AwayTeam = A and Season = ?)) ";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_BY_SEASON_IN_CASA = "select sum(HTHG) from matches where HomeTeam = A and Season = ?";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_BY_SEASON_FUORI_CASA = "select sum(HTAG) from matches where AwayTeam = A and Season = ?";
	
	// STATISTICHE DA UNA STAGIONE
	private static final String NUMERO_VITTORIE_A_SU_B_FROM_SEASON= "select count(*) as vittorieTotali from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG) and Season >= ?";
	private static final String NUMERO_VITTORIE_A_SU_B_FROM_SEASON_IN_CASA = "select count(*) as vittorieInCasa from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG and Season >= ?";
	private static final String NUMERO_VITTORIE_A_SU_B_FROM_SEASON_FUORI_CASA= "select count(*) as vittorieFuoriCasa from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG and Season >= ?";
	private static final String VITTORIE_A_SU_B_FROM_SEASON= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG) and Season >= ?";
	private static final String VITTORIE_A_SU_B_FROM_SEASON_IN_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG and Season >= ?";
	private static final String VITTORIE_A_SU_B_FROM_SEASON_FUORI_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG and Season >= ?";
	
	private static final String GOAL_TOTALI_PRIMO_TEMPO_FROM_SEASON = "select((select sum(HTHG) from matches where HomeTeam = A and Season >= ?) + (select sum(HTAG) from matches where AwayTeam = A and Season >= ?)) ";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_FROM_SEASON_IN_CASA = "select sum(HTHG) from matches where HomeTeam = A and Season >= ?";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_FROM_SEASON_FUORI_CASA = "select sum(HTAG) from matches where HomeTeam = A and Season >= ?";
	
	
	// STATISTICHE FINO AD UNA STAGIONE
	private static final String NUMERO_VITTORIE_A_SU_B_TO_SEASON= "select count(*) as vittorieTotali from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG) and Season <= ?";
	private static final String NUMERO_VITTORIE_A_SU_B_TO_SEASON_IN_CASA= "select count(*) as vittorieInCasa from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG and Season <= ?";
	private static final String NUMERO_VITTORIE_A_SU_B_TO_SEASON_FUORI_CASA= "select count(*) as vittorieFuoriCasa from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG and Season <= ?";
	private static final String VITTORIE_A_SU_B_TO_SEASON= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where (HomeTeam = A and AwayTeam = B and FTHG > FTAG) or (HomeTeam = B and AwayTeam = A and FTHG < FTAG) and Season <= ?";
	private static final String VITTORIE_A_SU_B_TO_SEASON_IN_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = A and AwayTeam = B and FTHG > FTAG and Season <= ?";
	private static final String VITTORIE_A_SU_B_TO_SEASON_FUORI_CASA= "select match_id, Season, HomeTeam, AwayTeam, FTHG, FTAG, HTHG, HTAG from matches where HomeTeam = B and AwayTeam = A and FTHG < FTAG and Season <= ?";

	private static final String GOAL_TOTALI_PRIMO_TEMPO_TO_SEASON = "select((select sum(HTHG) from matches where HomeTeam = A and Season <= ?) + (select sum(HTAG) from matches where AwayTeam = A and Season <= ?)) ";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_TO_SEASON_IN_CASA = "select sum(HTHG) from matches where HomeTeam = A and Season <= ?";
	private static final String GOAL_TOTALI_PRIMO_TEMPO_TO_SEASON_FUORI_CASA="select sum(HTAG) from matches where HomeTeam = A and Season <= ?";
	
	public List<Season> listSeasons() {
		String sql = "SELECT season, description FROM seasons";

		List<Season> result = new ArrayList<>();

		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);

			ResultSet res = st.executeQuery();

			while (res.next()) {
				result.add(new Season(res.getInt("season"), res.getString("description")));
			}

			conn.close();
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public List<Team> listTeams() {
		String sql = "SELECT team FROM teams";

		List<Team> result = new ArrayList<>();

		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);

			ResultSet res = st.executeQuery();

			while (res.next()) {
				result.add(new Team(res.getString("team")));
			}

			conn.close();
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public List<Match> listMatches(Map<String, Team> teamMap, Season s) {

		final String sql = "SELECT matches.match_id, matches.Season, matches.Div, matches.Date, matches.HomeTeam, matches.AwayTeam, matches.FTHG, matches.FTAG, matches.FTR"
				+ " FROM matches, teams as t1, teams as t2"
				+ " WHERE matches.HomeTeam = t1.team AND matches.AwayTeam = t2.team" + " AND matches.season = ?";

		List<Match> partite = new LinkedList<>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);

			st.setInt(1, s.getSeason());
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Team homeTeam = teamMap.get(rs.getString("matches.HomeTeam"));
				Team awayTeam = teamMap.get(rs.getString("matches.AwayTeam"));
				Match partita = new Match(rs.getInt("matches.match_id"), s, rs.getString("matches.Div"),
						rs.getDate("matches.Date").toLocalDate(), homeTeam, awayTeam, rs.getInt("matches.FTHG"),
						rs.getInt("matches.FTAG"), rs.getString("matches.FTR"));
				partite.add(partita);
			}

			st.close();
			conn.close();
			return partite;

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Map<String, Team> getTeamsForSeason(Season s) {

		final String sql = "SELECT DISTINCT t1.team, t2.team" + " FROM matches, teams as t1, teams as t2"
				+ " WHERE matches.HomeTeam = t1.team AND matches.AwayTeam = t2.team" + " AND matches.season = ?";

		Map<String, Team> squadre = new HashMap<>();

		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);

			st.setInt(1, s.getSeason());
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				if (!squadre.containsKey(rs.getString("t1.team"))) {
					Team team1 = new Team(rs.getString("t1.team"));
					squadre.put(rs.getString("t1.team"), team1);
				}

				if (!squadre.containsKey(rs.getString("t2.team"))) {
					Team team2 = new Team(rs.getString("t2.team"));
					squadre.put(rs.getString("t2.team"), team2);
				}

			}

			st.close();
			conn.close();
			return squadre;

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

}
