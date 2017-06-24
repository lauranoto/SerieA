/**
 * Sample Skeleton for 'SerieA.fxml' Controller Class
 */

package it.polito.tdp.seriea;

import java.net.URL;
import java.util.List;
import java.util.ResourceBundle;

import it.polito.tdp.seriea.model.Model;
import it.polito.tdp.seriea.model.Season;
import it.polito.tdp.seriea.model.Team;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TextArea;

public class SerieAController {

	@FXML // ResourceBundle that was given to the FXMLLoader
	private ResourceBundle resources;

	@FXML // URL location of the FXML file that was given to the FXMLLoader
	private URL location;

	@FXML // fx:id="boxSeason"
	private ChoiceBox<Season> boxSeason; // Value injected by FXMLLoader

	@FXML // fx:id="boxTeam"
	private ChoiceBox<Team> boxTeam; // Value injected by FXMLLoader

	@FXML // fx:id="txtResult"
	private TextArea txtResult; // Value injected by FXMLLoader

	private Model model;

	@FXML
	void handleCarica(ActionEvent event) {

		txtResult.clear();

		Season s = boxSeason.getValue();

		if (s == null) {
			txtResult.setText("Selezionare una stagione!\n");
			return;
		}

		model.creaGrafo(s);
		List<Team> classifica = model.calcolaClassifica(s);

		for (Team team : classifica) {
			txtResult.appendText(team.toString() + " punteggio: " + team.getPunteggio() + "\n");
		}

	}

	@FXML
	void handleDomino(ActionEvent event) {

		txtResult.clear();

		Season s = boxSeason.getValue();

		if (s == null) {
			txtResult.setText("Selezionare una stagione e caricare le partite!\n");
			return;
		}

		List<Team> domino = model.calcolaCammino(s);

		if (domino == null) {
			txtResult.setText("Impossibile trovare un domino\n");
		} else {
			for (int i=0; i<domino.size()-1; i++) {
				txtResult.appendText(domino.get(i).toString() + "-" + domino.get(i+1).toString() + "\n");
			}
		}
	}

	@FXML // This method is called by the FXMLLoader when initialization is
			// complete
	void initialize() {
		assert boxSeason != null : "fx:id=\"boxSeason\" was not injected: check your FXML file 'SerieA.fxml'.";
		assert boxTeam != null : "fx:id=\"boxTeam\" was not injected: check your FXML file 'SerieA.fxml'.";
		assert txtResult != null : "fx:id=\"txtResult\" was not injected: check your FXML file 'SerieA.fxml'.";
	}

	public void setModel(Model model) {
		this.model = model;
		boxSeason.getItems().addAll(model.getSeasons());

	}
}
