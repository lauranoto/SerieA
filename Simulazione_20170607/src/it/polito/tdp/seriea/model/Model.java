package it.polito.tdp.seriea.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import it.polito.tdp.seriea.db.SerieADAO;

public class Model {

	private SerieADAO dao;
	private List<Season> seasons;
	private SimpleDirectedWeightedGraph<Team, DefaultWeightedEdge> grafo;
	private List<Match> matches;
	private Map<String, Team> teamsForSeason;
	private List<Team> camminoMigliore;
	private Set<DefaultWeightedEdge> archiUsati;

	public Model() {
		super();
		this.dao = new SerieADAO();
		teamsForSeason = new HashMap<>();
	}

	public List<Season> getSeasons() {

		if (this.seasons == null) {
			seasons = dao.listSeasons();
		}
		return seasons;
	}

	public void creaGrafo(Season s) {

		grafo = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);

		this.teamsForSeason = dao.getTeamsForSeason(s);
		this.matches = dao.listMatches(teamsForSeason, s);

		Graphs.addAllVertices(grafo, teamsForSeason.values());

		for (Match match : matches) {
			DefaultWeightedEdge e = grafo.addEdge(match.getHomeTeam(), match.getAwayTeam());
			if (match.getFthg() > match.getFtag()) {
				grafo.setEdgeWeight(e, 1);
			} else if (match.getFthg() < match.getFtag()) {
				grafo.setEdgeWeight(e, -1);
			} else
				grafo.setEdgeWeight(e, 0);
		}

	}

	public List<Team> calcolaClassifica(Season s) {

		for (DefaultWeightedEdge e : this.getGrafo(s).edgeSet()) {
			if (grafo.getEdgeWeight(e) == 1) {
				grafo.getEdgeSource(e).incrementaPunteggio(3);
			} else if (grafo.getEdgeWeight(e) == 0) {
				grafo.getEdgeSource(e).incrementaPunteggio(1);
			}
		}

		List<Team> lista = new LinkedList<>(teamsForSeason.values());
		Collections.sort(lista);

		return lista;
	}

	public SimpleDirectedWeightedGraph<Team, DefaultWeightedEdge> getGrafo(Season s) {

		if (this.grafo == null) {
			creaGrafo(s);
		}
		return grafo;
	}

	public List<Team> calcolaCammino(Season s) {

		this.camminoMigliore = new ArrayList<>();
		this.archiUsati = new HashSet<>();

		List<Team> cammino = new ArrayList<>();

		this.riduciGrafo(8);

		for (Team partenza : grafo.vertexSet()) {
			cammino.add(partenza);
			recursive(1, partenza, cammino);
			cammino.remove(partenza);
		}

		return this.camminoMigliore;

	}

	private void recursive(int step, Team partenza, List<Team> cammino) {

		if (cammino.size() > this.camminoMigliore.size()) {
			camminoMigliore.clear();
			camminoMigliore.addAll(cammino);
		}

		// per ogni vertice di partenza provo ad aggiungere tutti i vertici a
		// cui è collegato
		for (DefaultWeightedEdge e : this.grafo.outgoingEdgesOf(partenza)) {
			Team arrivo = grafo.getEdgeTarget(e);

			// considero solo gli archi di peso = 1, li utilizzo una volta sola
			if (grafo.getEdgeWeight(e) == 1 && !this.archiUsati.contains(e)) {
				cammino.add(arrivo);
				archiUsati.add(e);
				recursive(step + 1, arrivo, cammino);
				archiUsati.remove(e);
				cammino.remove(cammino.size() - 1);
				// non rimuovo 'arrivo' perchè lo stesso vertice può comparire
				// più volte
			}
		}
	}

	private void riduciGrafo(int dim) {
		Set<Team> togliere = new HashSet<>();

		Iterator<Team> iter = grafo.vertexSet().iterator();
		for (int i = 0; i < grafo.vertexSet().size() - dim; i++) {
			togliere.add(iter.next());
		}
		grafo.removeAllVertices(togliere);
		System.err.println("Attenzione: cancello dei vertici dal grafo");
		System.err.println("Vertici rimasti: " + grafo.vertexSet().size() + "\n");
	}

}
