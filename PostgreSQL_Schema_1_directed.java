package de.bmager.storage.postgresql;

import de.bmager.database.PostgreDBConnector;
import de.bmager.experiment.misc.Tools;
import de.bmager.storage.AbstractStorage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * @author Konstantin Tiz
 */
public class PostgreSQL_Schema_1_directed extends AbstractStorage {

	private final static String INSERT_NODES_STMT = Tools.makeInsertStatement("nodes", 6);
	private final static String INSERT_EDGES_STMT = Tools.makeInsertStatement("edges", 3);
	private final static String INSERT_EDGEBODY_STMT = Tools.makeInsertStatement("edgebody", 3);
	private final static String SCHEMA_SCRIPT = "/home/ktiz2s/Schreibtisch/Mager/mcs-experiment-tool/resources/POSTGRE_CREATE_TABLES_SCHEMA_1.sql";

	@SuppressWarnings("deprecation")
	@Override
	public void storeGraph(File nodeFile, File edgeFile, boolean reset) throws Exception {

		if (!reset) {
			return;
		}

		PostgreDBConnector.db().setAutoCommit(false);

		// reset tables
		PostgreDBConnector.runScript(SCHEMA_SCRIPT);

		// insert nodes
		System.out.println("insert nodes");
		List<File> splittedFiles = Tools.splitNodeFile(nodeFile.getPath());
		try {
			int idCounter = 0;
			for (File currentFile : splittedFiles) {
				System.out.println("current file: " + currentFile.getName());
				PreparedStatement stmt = PostgreDBConnector.db().prepareStatement(INSERT_NODES_STMT);
				BufferedReader nodeFileReader = new BufferedReader(new FileReader(currentFile));
				nodeFileReader.readLine(); // skip header
				String nodeline;
				while ((nodeline = nodeFileReader.readLine()) != null) {
					String[] line = nodeline.split(",");
					stmt.setInt(1, idCounter++);
					stmt.setString(2, line[0]);
					stmt.setString(3, line[1]);
					stmt.setString(4, line[2]);
					Integer birthYear = Integer.parseInt(line[3].split("-")[0]);
					Integer birthMonth = Integer.parseInt(line[3].split("-")[1]);
					Integer birthDate = Integer.parseInt(line[3].split("-")[2]);
					stmt.setDate(5, new Date(birthYear, birthMonth, birthDate));
					stmt.setString(6, line[4]);
					stmt.addBatch();
				}
				stmt.executeBatch();
				stmt.close();
				nodeFileReader.close();
			}
		} catch (Exception e) {
			throw e;
		} finally {
			for (File currentFile : splittedFiles) {
				currentFile.delete();
			}
		}

		// insert edges
		System.out.println("insert edges");
		splittedFiles = Tools.splitEdgeFile(edgeFile.getPath());
		try {
			int edgeIdCounter = 0;
			for (File currentFile : splittedFiles) {
				System.out.println("current file: " + currentFile.getName());
				PreparedStatement edges_stmt = PostgreDBConnector.db().prepareStatement(INSERT_EDGES_STMT);
				PreparedStatement edgecontent_stmt = PostgreDBConnector.db().prepareStatement(INSERT_EDGEBODY_STMT);

				BufferedReader edgeFileReader = new BufferedReader(new FileReader(currentFile));
				String edgeline = edgeFileReader.readLine(); // skip header
				while ((edgeline = edgeFileReader.readLine()) != null) {

					String[] line = edgeline.split(",");

					// only one edge
					edges_stmt.setInt(1, Integer.parseInt(line[0]));
					edges_stmt.setInt(2, Integer.parseInt(line[1]));
					edges_stmt.setInt(3, edgeIdCounter);
					edges_stmt.addBatch();

					// content
					edgecontent_stmt.setInt(1, edgeIdCounter);
					edgecontent_stmt.setString(2, line[2]);
					Integer estYear = Integer.parseInt(line[3].split("-")[0]);
					Integer estMonth = Integer.parseInt(line[3].split("-")[1]);
					Integer estDate = Integer.parseInt(line[3].split("-")[2]);
					edgecontent_stmt.setDate(3, new Date(estYear, estMonth, estDate));
					edgecontent_stmt.addBatch();

					edgeIdCounter++;
				}
				edgeFileReader.close();
				edgecontent_stmt.executeBatch();
				edgecontent_stmt.close();
				edges_stmt.executeBatch();
				edges_stmt.close();
			}
		} catch (Exception e) {
			throw e;
		} finally {
			for (File currentFile : splittedFiles) {
				currentFile.delete();
			}
		}

		PostgreDBConnector.db().commit();
		PostgreDBConnector.db().setAutoCommit(true);
	}

	@Override
	public String getName() {
		return "postgresql";
	}

	@Override
	public String getSchemaDescription() {
		return "schema1-directed";
	}

}
