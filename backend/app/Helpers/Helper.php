<?php

namespace App\Helpers;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use DateTime;
use DatePeriod;
use DateInterval;

class Helper {

	public static function updateInplayTimerTable () {
		$curl = curl_init();
		$token = env("API_TOKEN", "");
		// Get inplay data
		$url = "https://api.b365api.com/v2/events/inplay?sport_id=1&token=$token";
		curl_setopt($curl, CURLOPT_URL, $url);
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
		$inplay_data = json_decode(curl_exec($curl), true);
		$inplay = array();
		if (array_key_exists("results", $inplay_data)) {
			$inplay = $inplay_data["results"];
		}
		// Get total inplay data
		if (count($inplay) > 0) {
			$total_page = intval($inplay_data["pager"]["total"] / $inplay_data["pager"]["per_page"]) + 1;
			for ($i = 2; $i <= $total_page; $i ++) {
				$url = "https://api.b365api.com/v2/events/inplay?sport_id=1&token=$token&day=$date&page=$i";
				curl_setopt($curl, CURLOPT_URL, $url);
				curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
				$inplay_data = json_decode(curl_exec($curl), true);
				if (array_key_exists("results", $inplay_data)) {
					$inplay = array_merge($inplay, $inplay_data["results"]);
				}   
				$request_count ++;
			}
		}

		$insert_data = array();
		foreach ($inplay as $data) {
			$event_id = $data['id'];
			$timer = $data["timer"]["tm"];
			$data = [
				'event_id' 	=>	$event_id,
				'timer'		=>	$timer,
			];
			array_push($insert_data, $data);
		}
		if (count($insert_data) > 0) {
			$inplayTimerTable = "f_inplay_timer";
			$insert_data_chunk = collect($insert_data);
			$chunks = $insert_data_chunk->chunk(200);
			$chunks_cnt = count($chunks);
			// truncate the inplay and upcoming table
			DB::table($inplayTimerTable)->truncate();
			foreach ($chunks as $chunk) {
				DB::table($inplayTimerTable)
						->insert($chunk->toArray());
			}
		}
	}

	public static function updateLeagueTable() {
		$football_league_table = "f_league";
		$curl = curl_init();
		$token = env("API_TOKEN", "");

		// Get league data
		$url = "https://api.b365api.com/v1/league?sport_id=1&token=$token";
		curl_setopt($curl, CURLOPT_URL, $url);
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
		$league_data = json_decode(curl_exec($curl), true);
		$leagues = array();
    	if (array_key_exists("results", $league_data)) {
      		$leagues = $league_data["results"];
			$total_page = intval($league_data["pager"]["total"] / $league_data["pager"]["per_page"]) + 1;
			for ($i = 2; $i <= $total_page; $i ++) {
				$url = "https://api.b365api.com/v1/league?sport_id=1&token=$token&page=$i";
				curl_setopt($curl, CURLOPT_URL, $url);
				curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
				$league_data = json_decode(curl_exec($curl), true);
				if (array_key_exists("results", $league_data)) {
					$leagues = array_merge($leagues, $league_data["results"]);
				}   
			}
    	}

		foreach ($leagues as $league) {
			if ((int)$league["has_leaguetable"] == 1) {
				$league_id = (int)$league["id"];
				$url = "https://api.b365api.com/v2/league/table?sport_id=1&token=$token&league_id=$league_id";
				curl_setopt($curl, CURLOPT_URL, $url);
				curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
				$league_table_data = json_decode(curl_exec($curl), true);
				if (array_key_exists("results", $league_table_data)
					&& array_key_exists("overall", $league_table_data["results"])
					&& array_key_exists("tables", $league_table_data["results"]["overall"])) {
					if (array_key_exists("season", $league_table_data["results"])) {
						if (array_key_exists("start_time", $league_table_data["results"]["season"])) {
							$start_time = $league_table_data["results"]["season"]["start_time"];
						} else {
							$start_time = NULL;
						}
						if (array_key_exists("end_time", $league_table_data["results"]["season"])) {
							$end_time = $league_table_data["results"]["season"]["end_time"];
						} else {
							$end_time = NULL;
						}
					}
					$groups = $league_table_data["results"]["overall"]["tables"];
					foreach ($groups as $group) {
						if (array_key_exists("rows", $group)) {
							foreach ($group["rows"] as $team) {
								if (array_key_exists("id", $team["team"])) {
									$team_id = (int)$team["team"]["id"];
									$update_or_insert_array = [
										"team_id"	=> $team_id,
										"team_name"	=> $team["team"]["name"],
										"rank"		=> (int)$team["pos"],
										"points"	=> (int)$team["points"],
										"league_id"	=> $league_id,
										"start_time"=> $start_time,
										"end_time"	=> $end_time,
									];
									DB::table($football_league_table)
										->updateOrInsert(
											[
												"team_id" 	=> $team_id,
												"league_id"	=> $league_id,
											],
											$update_or_insert_array
										);
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Import all historical data
	 * @param string $start_date
	 */
	public static function importHistoryData($start_date=NULL) {
    	$start = microtime(true);
    	if (!$start_date) {
      		$start_date = "20180101";
    	}
    	$period = new DatePeriod(
			new DateTime($start_date),
			new DateInterval('P1D'),
			date("Ymd")
    	);
    
    	foreach ($period as $key => $value) {
			Helper::updateDB($value->format("Ymd"), true);
		}
    	$execution_time = (microtime(true) - $start) / 60;
    	echo "Total Execution Time:  " . $execution_time. "  Mins";
  	}

	/**
	 * Get time period (human date -> timestamp) 00:00:00 ~ 23:59:59
	 * @param 	string 	$date
	 * @return 	array 	$times
	 */
  	public static function getTimePeriod($date) {
		if (!$date) {
			$date = date('Y-m-d', time());
		}

    	$times = array();
    	$d = DateTime::createFromFormat('Y-m-d H:i:s', $date . ' 00:00:00');
    	if ($d === false) {
      		die("Incorrect date string");
    	} else {
      		array_push($times, $d->getTimestamp());
    	}

    	$d = DateTime::createFromFormat('Y-m-d H:i:s', $date . ' 23:59:59');
    	if ($d === false) {
      		die("Incorrect date string");
		} else {
      		array_push($times, $d->getTimestamp());
    	}

    	return $times;
  	}

	/**
	 * Get matches (add some fields)
	 * @param 	array $matches_data_array
	 * @param 	array $leagues
	 * @return 	array $matches
	 */
	public static function getMatchesResponse($matches_data_array, $leagues, $match_status=-1) {
        $matches = array();
        $i = 0;
        foreach ($matches_data_array as $matches_data) {
            foreach ($matches_data as $data) {
                if (gettype($data) == "object") {
                    $data_id = $data->id;
                    $data_league_id = $data->league_id;
                    $data_league_name = $data->league_name;
                    $data_event_id = $data->event_id;
                    $data_home_id = $data->home_id;
                    $data_home_name = $data->home_name;
                    $data_home_odd = $data->home_odd;
                    $data_away_id = $data->away_id;
                    $data_away_name = $data->away_name;
                    $data_away_odd = $data->away_odd;
                    $data_scores = $data->scores;
                    $data_time_status = $data->time_status;
                    $data_time = $data->time;
                    $data_detail = $data->detail;
					// $data_timer = $data->timer;
                } else {
                    $data_id = $data["id"];
                    $data_league_id = $data["league_id"];
                    $data_league_name = $data["league_name"];
                    $data_event_id = $data["event_id"];
                    $data_home_id = $data["home_id"];
                    $data_home_name = $data["home_name"];
                    $data_home_odd = $data["home_odd"];
                    $data_away_id = $data["away_id"];
                    $data_away_name = $data["away_name"];
                    $data_away_odd = $data["away_odd"];
                    $data_scores = $data["scores"];
                    $data_time_status = $data["time_status"];
                    $data_time = $data["time"];
                    $data_detail = $data["detail"];
					// $data_timer = $data["timer"];
                }
    
                $matches[$i] = [
                    'id'             =>  $data_id,
                    'league_id'      =>  $data_league_id,
                    'league_name'    =>  $data_league_name,
                    'event_id'       =>  $data_event_id,
                    'home_id'        =>  $data_home_id,
                    'home_name'      =>  $data_home_name,
                    'home_odd'       =>  $data_home_odd,
                    'home_rank'   =>  "-",
                    'home_points'    =>  "-",
                    'away_id'        =>  $data_away_id,
                    'away_name'      =>  $data_away_name,
                    'away_odd'       =>  $data_away_odd,
                    'away_rank'   =>  "-",
                    'away_points'    =>  "-",
                    'scores'         =>  $data_scores,
                    'time_status'    =>  $data_time_status,
                    'time'           =>  $data_time,
                    'detail'         =>  $data_detail,
					// 'timer'			 =>	 $data_timer,
                ];
    
                foreach ($leagues as $league) {
					if ($data_league_id == $league->league_id && $data_home_id == $league->team_id) {
						$matches[$i]["home_name"] = $league->team_name;
                        $matches[$i]["home_rank"] = $league->rank;
                        $matches[$i]["home_points"] = $league->points;
					}
                    if ($data_league_id == $league->league_id && $data_away_id == $league->team_id) {
						$matches[$i]["away_name"] = $league->team_name;
                        $matches[$i]["away_rank"] = $league->rank;
                        $matches[$i]["away_points"] = $league->points;
					}
                }
                $i ++;
            }
        }
        return $matches;
    }

	/**
     * Get relation data (pre-calcuated)
     * @param   array   $player_ids
     * @return  array   $matches
     */
    public static function getRelationMatches($team_ids, $history_tables, $leagues, $league_id, $date, $calculate_type=1) {
		$teams_array = array();
		$request_times = Helper::getTimePeriod($date);
		foreach ($team_ids as $team_id) {
			$matches_array = array();
			foreach ($history_tables as $table) {
				// filtering by player id
				$match_table_subquery = DB::table($table->tablename)
											->where('time_status', 3)
											->where('scores', '<>', NULL)
											->where('scores', '<>', '')
											->where('league_id', $league_id)
											->where('time', '<=', $request_times[1])
											->where(function($query) use ($team_id) {
												$query->where('home_id', $team_id)
												->orWhere('away_id', $team_id);
											});
				
				array_push($matches_array, $match_table_subquery->get());
			}
	
			$matches = Helper::getMatchesResponse($matches_array, $leagues);
			$teams_array[$team_id] = $matches;
		}
		
		return $teams_array;
    }

	/**
	 * Import the historical data of a specific day
	 * @param string $date
	 * @param bool $once
	 */
  	public static function updateDB($date=false, $once=false, $match_status=3) {
		$start_time = microtime(true);
		$request_count = 0;
		if (!$date) {
			$date = date("Ymd");
		}

		if ($date > date("Ymd")) {
			return;
		}
		$log = substr($date, 0, 4) . "-" . substr($date, 4, 2) . "-" . substr($date, 6, 2) . ":  Start Time: " . date("Y-m-d H:i:s");
    	// Check Y-m table is exist or not
		if ($match_status == 3) {
			$match_table_name = "f_matches_" . substr($date, 0, 4) . "_" . substr($date, 4, 2);
		} else if ($match_status == 0) {
			$match_table_name = "f_upcoming";
		} else if ($match_status == 1) {
			$match_table_name = "f_inplay";
		}

		$inplayUpcomingMatches = array();
		$event_ids = array();
    	if (!Schema::hasTable($match_table_name)) {
      		// Code to create table
      		Schema::create($match_table_name, function($table) {
				$table->increments("id");
				$table->integer("event_id");
				$table->integer("league_id");
				$table->string("league_name", 255);
				$table->integer("home_id");
				$table->string("home_name", 255);
				$table->float("home_odd")->nullable();
				$table->integer("away_id");
				$table->string("away_name", 255);
				$table->float("away_odd")->nullable();
				$table->string("scores", 10)->nullable();
				$table->integer("time_status");
				$table->integer("time");
				$table->text("detail")->nullable();
				$table->index(['home_id', 'away_id']);
			});
    	}

		$curl = curl_init();
		$token = env("API_TOKEN", "");

		$history_total_count = 0;
		if ($match_status == 3) {
			// Get history data
			$url = "https://api.b365api.com/v2/events/ended?sport_id=1&token=$token&day=$date";
			curl_setopt($curl, CURLOPT_URL, $url);
			curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
			$history_data = json_decode(curl_exec($curl), true);
			$history = array();
			if ($history_data != NULL && array_key_exists("results", $history_data)) {
				  $history = $history_data["results"];
			}
			// Get total history data
			if (count($history) > 0) {
				$total_page = intval($history_data["pager"]["total"] / $history_data["pager"]["per_page"]) + 1;
				$history_total_count = $history_data["pager"]["total"];
				  for ($i = 2; $i <= $total_page; $i ++) {
					$url = "https://api.b365api.com/v2/events/ended?sport_id=1&token=$token&day=$date&page=$i";
					curl_setopt($curl, CURLOPT_URL, $url);
					curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
					$history_data = json_decode(curl_exec($curl), true);
					if ($history_data != NULL && array_key_exists("results", $history_data)) {
						  $history = array_merge($history, $history_data["results"]);
					}
					$request_count ++;
				  }
			}
      		$matches = $history;
    	} else if ($match_status == 0) {
			// Get upcoming data
			$url = "https://api.b365api.com/v2/events/upcoming?sport_id=1&token=$token";
			curl_setopt($curl, CURLOPT_URL, $url);
			curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
			$upcoming_data = json_decode(curl_exec($curl), true);
			$upcoming = array();
			if (array_key_exists("results", $upcoming_data)) {
				$upcoming = $upcoming_data["results"];
			}
			$request_count ++;
			// Get total upcoming data
			if (count($upcoming) > 0) {
				$total_page = intval($upcoming_data["pager"]["total"] / $upcoming_data["pager"]["per_page"]) + 1;
				for ($i = 2; $i <= $total_page; $i ++) {
					$url = "https://api.b365api.com/v2/events/upcoming?sport_id=1&token=$token&day=$date&page=$i";
					curl_setopt($curl, CURLOPT_URL, $url);
					curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
					$upcoming_data = json_decode(curl_exec($curl), true);
					if (array_key_exists("results", $upcoming_data)) {
						$upcoming = array_merge($upcoming, $upcoming_data["results"]);
					}   
					$request_count ++;
				}
			}
			$matches = $upcoming;
		} else if ($match_status == 1) {
			// Get inplay data
			$url = "https://api.b365api.com/v2/events/inplay?sport_id=1&token=$token";
			curl_setopt($curl, CURLOPT_URL, $url);
			curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
			$inplay_data = json_decode(curl_exec($curl), true);
			$inplay = array();
      		if (array_key_exists("results", $inplay_data)) {
				$inplay = $inplay_data["results"];
      		}
      		$request_count ++;
      		// Get total inplay data
      		if (count($inplay) > 0) {
        		$total_page = intval($inplay_data["pager"]["total"] / $inplay_data["pager"]["per_page"]) + 1;
        		for ($i = 2; $i <= $total_page; $i ++) {
					$url = "https://api.b365api.com/v2/events/inplay?sport_id=1&token=$token&day=$date&page=$i";
					curl_setopt($curl, CURLOPT_URL, $url);
					curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
					$inplay_data = json_decode(curl_exec($curl), true);
					if (array_key_exists("results", $inplay_data)) {
						$inplay = array_merge($inplay, $inplay_data["results"]);
					}   
					$request_count ++;
				}
      		}
      		$matches = $inplay;
    	}

		if (count($matches) > 0) {
			foreach ($matches as $event) {
				if (!in_array($event["id"], $event_ids)) {
					$league_name = $event["league"]["name"];
					if (!str_contains($league_name, 'Esoccer')) {
						array_push($event_ids, $event["id"]);
					}
				}
			}
		}

		foreach ($event_ids as $event_id) {
			// Get Odds
			$url = "https://api.b365api.com/v2/event/odds/summary?token=$token&event_id=$event_id";
			$request_count ++;
			curl_setopt($curl, CURLOPT_URL, $url);
			curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
			$summary = json_decode(curl_exec($curl), true);
			// Get surface
			$url = "https://api.b365api.com/v1/event/view?token=$token&event_id=$event_id";
			$request_count ++;
			curl_setopt($curl, CURLOPT_URL, $url);
			curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
			$view = json_decode(curl_exec($curl), true);
			if ($view != NULL && array_key_exists("results", $view) && count($view["results"]) > 0 && $view["results"][0]["id"] == $event_id) {
				$event = $view["results"][0];
				$timer = '';
				if ($event != NULL && array_key_exists("home", $event) && (array_key_exists("away", $event) || array_key_exists("o_away", $event))) {
					$league_id = (int)$event["league"]["id"];
					$league_name = $event["league"]["name"];
					if ($match_status == 1) {
						$timer = $event["timer"]["tm"];
					}
					if (!str_contains($league_name, 'Esoccer')) {
						$time_status = (int)$event["time_status"];
						if ($time_status == 0 || $time_status == 1 || $time_status == 3) {
							$home_id = array_key_exists("o_home", $event) ? (int)$event["o_home"]["id"] : (int)$event["home"]["id"];
							$home_name = array_key_exists("o_home", $event) ? $event["o_home"]["name"] : $event["home"]["name"];
							$home_odd = NULL;
			
							$away_id = array_key_exists("o_away", $event) ? (int)$event["o_away"]["id"] : (int)$event["away"]["id"];
							$away_name = array_key_exists("o_away", $event) ? $event["o_away"]["name"] : $event["away"]["name"];
							$away_odd = NULL;
			
							if ($summary && array_key_exists("results", $summary) && array_key_exists("Bet365", $summary["results"])) {
								if ($time_status == 3) {
									if (array_key_exists("kickoff", $summary["results"]["Bet365"]["odds_update"])) {
										$kickoff = $summary["results"]["Bet365"]["odds_update"]["kickoff"];
									} else {
										$kickoff = $summary["results"]["Bet365"]["odds"]["kickoff"];
									}
	
									if (array_key_exists("1_1", $kickoff) && $kickoff["1_1"] !== NULL) {
										$home_odd = array_key_exists("home_od", $kickoff["1_1"]) ? $kickoff["1_1"]["home_od"] : NULL;
										$away_odd = array_key_exists("away_od", $kickoff["1_1"]) ? $kickoff["1_1"]["away_od"] : NULL;
									} elseif (array_key_exists("1_2", $kickoff) && $kickoff["1_2"] !== NULL) {
										$home_odd = array_key_exists("home_od", $kickoff["1_2"]) ? $kickoff["1_2"]["home_od"] : NULL;
										$away_odd = array_key_exists("away_od", $kickoff["1_2"]) ? $kickoff["1_2"]["away_od"] : NULL;
									} elseif (array_key_exists("1_3", $kickoff) && $kickoff["1_3"] !== NULL) {
										$home_odd = array_key_exists("home_od", $kickoff["1_3"]) ? $kickoff["1_3"]["home_od"] : NULL;
										$away_odd = array_key_exists("away_od", $kickoff["1_3"]) ? $kickoff["1_3"]["away_od"] : NULL;
									}
								} else {
									if (array_key_exists("start", $summary["results"]["Bet365"]["odds_update"])) {
										$start = $summary["results"]["Bet365"]["odds_update"]["start"];
									} else {
										$start = $summary["results"]["Bet365"]["odds"]["start"];
									}
									if (array_key_exists("1_1", $start) && $start["1_1"] !== NULL) {
										$home_odd = array_key_exists("home_od", $start["1_1"]) ? $start["1_1"]["home_od"] : NULL;
										$away_odd = array_key_exists("away_od", $start["1_1"]) ? $start["1_1"]["away_od"] : NULL;
									} elseif (array_key_exists("1_2", $start) && $start["1_2"] !== NULL) {
										$home_odd = array_key_exists("home_od", $start["1_2"]) ? $start["1_2"]["home_od"] : NULL;
										$away_odd = array_key_exists("away_od", $start["1_2"]) ? $start["1_2"]["away_od"] : NULL;
									} elseif (array_key_exists("1_3", $start) && $start["1_3"] !== NULL) {
										$home_odd = array_key_exists("home_od", $start["1_3"]) ? $start["1_3"]["home_od"] : NULL;
										$away_odd = array_key_exists("away_od", $start["1_3"]) ? $start["1_3"]["away_od"] : NULL;
									}
								}
							}
	
							$detail = "";
							if (array_key_exists("events", $event)) {
								foreach ($event["events"] as $game) {
									if (strpos($game["text"], " Goal ") !== false && strpos($game["text"], " Kicks ") == false) {
										$splitedText = explode(" ", $game["text"]);
										if (count($splitedText) > 2) {
											$detail .= substr($splitedText[0], 0, -1);
											$detail .= ":";
											if (strpos($game["text"], $home_name) !== false) {
												$detail .= "1";
											} else {
												$detail .= "2";
											}
											$detail .= ",";
										}
									}
								}
							}
							if ($detail != "") {
								$detail = substr($detail, 0, -1);
							}
							$update_or_insert_array = [
								"event_id"   	=> (int)$event_id,
								"league_id"   	=> $league_id,
								"league_name" 	=> $league_name,
								"home_id"    	=> $home_id,
								"home_name"  	=> $home_name,
								"home_odd"   	=> $home_odd,
								"away_id"    	=> $away_id,
								"away_name"  	=> $away_name,
								"away_odd"   	=> $away_odd,
								"scores"      	=> $event["ss"] ? trim($event["ss"]) : "",
								"time_status" 	=> $time_status,
								"time"        	=> (int)$event["time"],
								"detail"		=> $detail
							];
							if ($time_status == 0 || $time_status == 1) {
								if ($match_status == 1) {
									$update_or_insert_array["timer"] = $timer;
								}
								array_push($inplayUpcomingMatches, $update_or_insert_array);
							} else {
								if ($match_status == 3) {
									// Update or Insert into f_matches table
									DB::table($match_table_name)
										->updateOrInsert(
											["event_id" => (int)$event_id],
											$update_or_insert_array
									);
								}
							}
						}
					}
				}
			}
		}
		if ($match_status == 0 || $match_status == 1) {
			$insert_data = collect($inplayUpcomingMatches);
			$chunks = $insert_data->chunk(300);
			$chunks_cnt = count($chunks);
			// truncate the inplay and upcoming table
			DB::table($match_table_name)->truncate();
			foreach ($chunks as $chunk) {
				DB::table($match_table_name)
						->insert($chunk->toArray());
			}
		} else if ($match_status == 3) {
			$execution_time = microtime(true) - $start_time;
			$log .= ("  End Time: " . date("Y-m-d H:i:s"));
			$log .= ("  ===>  Total History Count: " . $history_total_count . ", Execution Time:  " . $execution_time. " secs, Request Count: " . $request_count . "\n");
			echo $log;
		}
		curl_close($curl);
	}

	public static function printLog($content) {
		echo $content . "\n";
	}

	/**
	 * Create backtest tables for the historical matches from 2021-04-05 ~ 2021-04-11
	 */
	public static function generateDataForBacktestRobots() {
		$start_time = microtime(true);
		$backtest_players_table = "t_backtest_players";

		/* --- Get player ids between 2021-04-05 and 2021-04-11 --- start --- */
		// get events
		$events = DB::table("t_matches_2021_03")
					->where("time_status", 3)
					->get();

		$player_ids = array();
		foreach ($events as $event) {
			if (!in_array($event->player1_id, $player_ids)) {
				array_push($player_ids, $event->player1_id);
			}
			if (!in_array($event->player2_id, $player_ids)) {
				array_push($player_ids, $event->player2_id);
			}
		}
		/* --- Get player ids between 2021-04-05 and 2021-04-11 ---  end  --- */
		$log = "Count of players: " . count($player_ids);
		Helper::printLog($log);

		$players = DB::table("t_players")->get();
		$history_tables = DB::table("pg_catalog.pg_tables")
								->where("schemaname", "public")
								->where("tablename", "like", "t_matches_%")
								->get();

		/* --- Create backtest players table --- start --- */
		Schema::dropIfExists($backtest_players_table);
		Schema::create($backtest_players_table, function($table) {
			$table->increments("id");
			$table->integer("event_id");
			$table->integer("p_id");
			$table->string("p_name", 100);
			$table->float("p_odd")->nullable();
			$table->integer("p_ranking")->nullable();
			$table->json("p_brw");
			$table->json("p_brl");
			$table->json("p_gah");
			$table->json("p_depths");
			$table->json("p_ww");
			$table->json("p_wl");
			$table->json("p_lw");
			$table->json("p_ll");
			$table->integer("o_id");
			$table->string("o_name", 100);
			$table->float("o_odd")->nullable();
			$table->integer("o_ranking")->nullable();
			$table->string("scores", 50)->nullable();
			$table->string("surface", 50)->nullable();
			$table->integer("time");
			$table->text("detail")->nullable();
			$table->string("home", 1);
		});
		/* --- Create backtest players table ---  end  --- */

		$relation_data = Helper::getRelationMatches($player_ids, $history_tables, $players, 0);
		// insert t_backtest_players table
		$relation_players_object = $relation_data[0];
		$players_object = array();
		foreach ($relation_players_object as $player_object) {
			$players_object = array_merge($players_object, $player_object);
		}

		/* --- insert players data --- start --- */
		$total_player_count = count($players_object);
		$log = "players count: " . $total_player_count;
		Helper::printLog($log);
		$player_data = collect($players_object);
		$chunks = $player_data->chunk(500);
		$i = 0;
		$chunks_cnt = count($chunks);
		foreach ($chunks as $chunk) {
			DB::table($backtest_players_table)
					->insert($chunk->toArray());
			$i ++;
			$log = "player chunk ended: " . $chunks_cnt . " / " . $i;
			Helper::printLog($log);
		}
		/* --- insert players data ---  end  --- */

		$execution_time = microtime(true) - $start_time;
		$log .= ("  End: " . date("Y-m-d H:i:s"));
		$log .= "  ExecutionTime: " . $execution_time;
		Helper::printLog($log);
	}

	public static function getWinners($matches, $matchType=0) {
		/**
		 * Robot 43: BRW + GAH + RANK + L10
		 * Robot 44: BRW + GAH + RANK + L20
		 */
		$players = DB::table("t_players")->get();
		$history_tables = DB::table("pg_catalog.pg_tables")
								->where("schemaname", "public")
								->where("tablename", "like", "t_matches_%")
								->get();

		$filteredMatches = Helper::filterMatchesByRankOdd($matches);
		$winners = array();
		$event_ids = array();
        $correct = 0;
		foreach ($filteredMatches as $match) {
			$player1_id = $match["player1_id"];
			$player2_id = $match["player2_id"];
			$player_detail = [
				"player1_odd" => $match["player1_odd"],
				"player2_odd" => $match["player2_odd"],
				"player1_ranking" => $match["player1_ranking"],
				"player2_ranking" => $match["player2_ranking"],
			];
			
			if ($matchType == 0) { // history so we have to pre-calculate
				$player_ids = [$player1_id, $player2_id];
				$relation_data = Helper::getRelationMatches($player_ids, $history_tables, $players, 0);
				
				$player1_object = $relation_data[0][0];
				usort($player1_object, function($a, $b) {
					return $a['time'] - $b['time'];
				});
				$player1_object = Helper::getUniqueMatchesByEventId($player1_object);
				$player1_objects_l_10 = array_slice($player1_object, 0, 10);
				$player1_object = Helper::getUniqueMatchesByEventId($player1_object);
				$player1_objects_l_20 = array_slice($player1_object, 0, 20);

				$player2_object = $relation_data[0][1];
				usort($player2_object, function($a, $b) {
					return $a['time'] - $b['time'];
				});
				$player2_object = Helper::getUniqueMatchesByEventId($player2_object);
				$player2_objects_l_10 = array_slice($player2_object, 0, 10);
				$player2_object = Helper::getUniqueMatchesByEventId($player2_object);
				$player2_objects_l_20 = array_slice($player2_object, 0, 20);
			} else { // inplay or upcoming so we can use pre-calculation table directly
				$table_name = "t_bucket_players_" . $player1_id . "_" . $player2_id;
				// for robot 41 and 43
				$player1_objects_l_10 = DB::table($table_name)
											->select("event_id", "p_brw", "p_gah")
											->where("p_id", $player1_id)
											->orderByDesc("time")
											->get();
				$player2_objects_l_10 = DB::table($table_name)
											->select("event_id", "p_brw", "p_gah")
											->where("p_id", $player2_id)
											->orderByDesc("time")
											->get();
				// for robot 42 and 44
				$player1_objects_l_20 = DB::table($table_name)
											->select("event_id", "p_brw", "p_gah")
											->where("p_id", $player1_id)
											->orderByDesc("time")
											->get();
				$player2_objects_l_20 = DB::table($table_name)
											->select("event_id", "p_brw", "p_gah")
											->where("p_id", $player2_id)
											->orderByDesc("time")
											->get();
				$player1_objects_l_10 = Helper::getUniqueMatchesByEventId($player1_objects_l_10, 10);
				$player1_objects_l_20 = Helper::getUniqueMatchesByEventId($player1_objects_l_20, 20);
				$player2_objects_l_10 = Helper::getUniqueMatchesByEventId($player2_objects_l_10, 10);
				$player2_objects_l_20 = Helper::getUniqueMatchesByEventId($player2_objects_l_20, 20);
			}

			if ($player_detail["player1_odd"] != NULL && $player_detail["player2_odd"] != NULL && $player_detail["player1_ranking"] != "-" && $player_detail["player2_ranking"] != "-") {
				if ($matchType == 0) {
                    $win_1 = 0;
                    $win_2 = 0;
                    $scores = explode(",", $match['scores']);
                    foreach ($scores as $score) {
                        $set_score = explode("-", $score);
                        if (count($set_score) == 2) {
                            $diff = (int)$set_score[0] - (int)$set_score[1];
                            if ($diff >= 0) {
                                $win_1 ++;
                            } else {
                                $win_2 ++;
                            }
                        }
                    }
                }
                // for robot 44 (BRW + GAH + RANK + L20)
				$winner_44 = Helper::robot4344($player1_objects_l_20, $player2_objects_l_20, 20, $player_detail);
				if ($winner_44 != 0) {
					if ($matchType == 0) {
                        if (($win_1 >= $win_2 && $winner_44 == 1) || ($win_1 <= $win_2 && $winner_44 == 2)) {
                            $correct = 1;
                        } else {
                            $correct = -1;
                        }
                    }
                    $detail = [
						'event_id'  => $match['event_id'],
						'winner'    => $winner_44,
						'type'      => 44,
                        'correct'   => $correct,
					];
					if (!in_array($match['event_id'], $event_ids)) {
						array_push($winners, $detail);
						array_push($event_ids, $match['event_id']);
					}
				}
				// for robot 43 (BRW + GAH + RANK + L10)
				$winner_43 = Helper::robot4344($player1_objects_l_10, $player2_objects_l_10, 10, $player_detail);
				if ($winner_43 != 0) {
                    if ($matchType == 0) {
                        if (($win_1 >= $win_2 && $winner_43 == 1) || ($win_1 <= $win_2 && $winner_43 == 2)) {
                            $correct = 1;
                        } else {
                            $correct = -1;
                        }
                    }
					$detail = [
						'event_id'  => $match['event_id'],
						'winner'    => $winner_43,
						'type'      => 43,
                        'correct'   => $correct,
					];
					if (!in_array($match['event_id'], $event_ids)) {
						array_push($winners, $detail);
						array_push($event_ids, $match['event_id']);
					}
				}
			}
		}
		return $winners;
	}

	public static function getUniqueMatchesByEventId($events, $limit=-1) {
		$event_ids = array();
		$newEvents = array();
		$i = 0;
		foreach ($events as $event) {
			if (gettype($event) == "object") {
				$event_id = $event->event_id;
			} else {
				$event_id = $event["event_id"];
			}
			if (!in_array($event_id, $event_ids)) {
				array_push($newEvents, $event);
				array_push($event_ids, $event_id);
				if ($limit != -1) {
					$i ++;
					if ($i == $limit) {
						break;
					}
				}
			}
		}
		return $newEvents;
	}

	public static function getUniqueMatchesByODetail($events, $limit=-1) {
		$event_ids = array();
		$o_ids = array();
		$oo_ids = array();
		$newEvents = array();
		$i = 0;
		foreach ($events as $event) {
			$insert = false;
			$index = array_search($event->event_id, $event_ids);
			if ($index == false) {
				$insert = true;
			} else {
				if ($o_ids[$index] == $event->o_id && $oo_ids[$index] == $event->oo_id) {
					$insert = false;
				} else {
					$insert = true;
				}
			}
			if ($insert) {
				array_push($newEvents, $event);
				array_push($event_ids, $event->event_id);
				array_push($o_ids, $event->o_id);
				array_push($oo_ids, $event->oo_id);
				if ($limit != -1) {
					$i ++;
					if ($i == $limit) {
						break;
					}
				}
			}
		}
		return $newEvents;
	}

	public static function robot4344($player1_events, $player2_events, $limit, $player_detail) {
		$player1_ranking = $player_detail["player1_ranking"];
		$player2_ranking = $player_detail["player2_ranking"];
		$player1_gah = 0;
		$player1_brw = 0;
		$i = 0;
		foreach ($player1_events as $player1_event) {
			if (gettype($player1_event) == "object") {
				$p_brws = json_decode($player1_event->p_brw);
				$p_gahs = json_decode($player1_event->p_gah);
			} else {
				$p_brws = json_decode($player1_event["p_brw"]);
				$p_gahs = json_decode($player1_event["p_gah"]);
			}
			foreach ($p_brws as $p_brw) {
				$player1_brw += array_sum($p_brw);
			}
			foreach ($p_gahs as $p_gah) {
				$player1_gah += array_sum($p_gah);
			}
			$i ++;
			if ($i == $limit) {
				break;
			}
		}

		$player2_gah = 0;
		$player2_brw = 0;
		$i = 0;
		foreach ($player2_events as $player2_event) {
			if (gettype($player2_event) == "object") {
				$p_brws = json_decode($player2_event->p_brw);
				$p_gahs = json_decode($player2_event->p_gah);
			} else {
				$p_brws = json_decode($player2_event["p_brw"]);
				$p_gahs = json_decode($player2_event["p_gah"]);
			}
			foreach ($p_brws as $p_brw) {
				$player2_brw += array_sum($p_brw);
			}
			foreach ($p_gahs as $p_gah) {
				$player2_gah += array_sum($p_gah);
			}
			$i ++;
			if ($i == $limit) {
				break;
			}
		}
		$expected_winner = ($player1_brw + $player1_gah) >= ($player2_brw + $player2_gah) ? 1 : 2;
		if (($expected_winner == 1 && $player1_ranking < $player2_ranking) || ($expected_winner == 2 && $player2_ranking < $player1_ranking)) {
			return $expected_winner;
		} else {
			return 0;
		}
	}

	/**
	 * Get the matches that have rank and odd
	 */
	public static function filterMatchesByRankOdd ($matches) {
		$filteredMatches = array();
		foreach ($matches as $match) {
			$player1_ranking = $match["player1_ranking"];
			$player2_ranking = $match["player2_ranking"];
			$player1_odd = $match["player1_odd"];
			$player2_odd = $match["player2_odd"];
			if (($player1_ranking != "-" && $player2_ranking != "-" && $player1_odd != NULL && $player2_odd != NULL) ||
			($player1_ranking == "-" && $player2_ranking == "-")) {
				array_push($filteredMatches, $match);
			}
		}
		return $filteredMatches;
	}

	/**
	 * Create t_backtest_bots_ ... about the strategies
	 */
	public static function robotStrategies() {
		$backtest_players_table = "t_backtest_players";
		$enable_robots = [
			1, 1,
		];
		/* --- Create t_backtest_bots_brw_10 table --- start --- */
		$backtest_bots = [
			"t_backtest_bots_brw_gah_rank_10",
			"t_backtest_bots_brw_gah_rank_20",
		];

		// BRW + GAH + RANK (Lower) (Ranked)
		if ($enable_robots[42]) {
			Schema::dropIfExists($backtest_bots[42]);
			Schema::create($backtest_bots[42], function($table) {
				$table->increments("id");
				$table->integer("event_id");
				$table->integer("p1_brw");
				$table->integer("p2_brw");
				$table->integer("p1_gah");
				$table->integer("p2_gah");
				$table->integer("p1_rank");
				$table->integer("p2_rank");
				$table->integer("expected_winner");
				$table->integer("real_winner");
			});
		}
		if ($enable_robots[43]) {
			Schema::dropIfExists($backtest_bots[43]);
			Schema::create($backtest_bots[43], function($table) {
				$table->increments("id");
				$table->integer("event_id");
				$table->integer("p1_brw");
				$table->integer("p2_brw");
				$table->integer("p1_gah");
				$table->integer("p2_gah");
				$table->integer("p1_rank");
				$table->integer("p2_rank");
				$table->integer("expected_winner");
				$table->integer("real_winner");
			});
		}

		// get events
		$events = DB::table("t_matches_2021_03")
					->where("time_status", 3)
					->get();
		$total_event_cnt = count($events);
		$log = "Total events: " . $total_event_cnt;
		Helper::printLog($log);

		$players = DB::table("t_players")->get();
		$player_ids = array();
		$player_ranks = array();
		foreach ($players as $player) {
			array_push($player_ids, $player->api_id);
			array_push($player_ranks, $player->ranking);
		}
		$event_cnt = 0;
		foreach ($events as $event) {
			$event_id = $event->event_id; 
			$surface = $event->surface;
			$player1_id = $event->player1_id;
			$player2_id = $event->player2_id;
			$player1_odd = $event->player1_odd;
			$player2_odd = $event->player2_odd;
			if ($player1_odd != NULL) {
				$player1_odd = (float)$player1_odd;
			}
			if ($player2_odd != NULL) {
				$player2_odd = (float)$player2_odd;
			}

			if ($event->scores != "") {
				$scores = explode(",", $event->scores);
				$scores = explode("-", $scores[0]);
				if (count($scores) == 2) {
					if ((int)$scores[0] > (int)$scores[1]) {
						$real_winner = 1;
					} else {
						$real_winner = 2;
					}

					$weekday = date('N', $event->time); 

					if (in_array($player1_id, $player_ids)) {
						$key_1 = array_search($player1_id, $player_ids);
						$player1_ranking = $player_ranks[$key_1];
					} else {
						$player1_ranking = 501;
					}
					if (in_array($player2_id, $player_ids)) {
						$key_2 = array_search($player2_id, $player_ids);
						$player2_ranking = $player_ranks[$key_2];
					} else {
						$player2_ranking = 501;
					}
	
					$player1_events = DB::table($backtest_players_table)
											->select("event_id", "p_brw", "p_brl", "p_gah")
											->where("p_id", $player1_id)
											->orderByDesc("time")
											->get();
					$player1_events = Helper::getUniqueMatchesByEventId($player1_events, 30);

					$player1_surface_events = DB::table($backtest_players_table)
													->select("event_id", "p_brw", "p_brl", "p_gah")
													->where("p_id", $player1_id)
													->where("surface", $surface)
													->orderByDesc("time")
													->get();
					$player1_surface_events = Helper::getUniqueMatchesByEventId($player1_surface_events, 30);

					$player2_events = DB::table($backtest_players_table)
											->select("event_id", "p_brw", "p_brl", "p_gah")
											->where("p_id", $player2_id)
											->orderByDesc("time")
											->get();
					$player2_events = Helper::getUniqueMatchesByEventId($player2_events, 30);

					$player2_surface_events = DB::table($backtest_players_table)
													->select("event_id", "p_brw", "p_brl", "p_gah")
													->where("p_id", $player2_id)
													->where("surface", $surface)
													->orderByDesc("time")
													->get();
					$player1_surface_events = Helper::getUniqueMatchesByEventId($player1_surface_events, 30);

					/* --- strategy 43 (BRW + GAH + RANK + L10) --- start --- */
					if ($enable_robots[42] &&
						$player1_odd != NULL && $player2_odd != NULL &&
						// (($player1_odd >= 1.7 && $player1_odd <= 2) || ($player2_odd >= 1.7 && $player2_odd <= 2)) &&
						$player1_ranking != 501 && $player2_ranking != 501
					) {
						$player1_gah = 0;
						$player1_brw = 0;
						$i = 0;
						foreach ($player1_events as $player1_event) {
							$p_brws = json_decode($player1_event->p_brw);
							foreach ($p_brws as $p_brw) {
								$player1_brw += array_sum($p_brw);
							}
							$p_gahs = json_decode($player1_event->p_gah);
							foreach ($p_gahs as $p_gah) {
								$player1_gah += array_sum($p_gah);
							}
							$i ++;
							if ($i == 10) {
								break;
							}
						}
			
						$player2_gah = 0;
						$player2_brw = 0;
						$i = 0;
						foreach ($player2_events as $player2_event) {
							$p_brws = json_decode($player2_event->p_brw);
							foreach ($p_brws as $p_brw) {
								$player2_brw += array_sum($p_brw);
							}
							$p_gahs = json_decode($player2_event->p_gah);
							foreach ($p_gahs as $p_gah) {
								$player2_gah += array_sum($p_gah);
							}
							$i ++;
							if ($i == 10) {
								break;
							}
						}
						$expected_winner = ($player1_brw + $player1_gah) >= ($player2_brw + $player2_gah) ? 1 : 2;
						if (($expected_winner == 1 && $player1_ranking < $player2_ranking) || ($expected_winner == 2 && $player2_ranking < $player1_ranking)) {
							$insert_data = [
								"event_id" 			=> $event_id,
								"p1_brw" 			=> $player1_brw,
								"p2_brw" 			=> $player2_brw,
								"p1_gah" 			=> $player1_gah,
								"p2_gah" 			=> $player2_gah,
								"p1_rank" 			=> $player1_ranking,
								"p2_rank" 			=> $player2_ranking,
								"expected_winner" 	=> $expected_winner,
								"real_winner"		=> $real_winner,
							];
							DB::table($backtest_bots[42])
								->insert($insert_data);
						}
					}
					/* --- strategy 43 (BRW + GAH + RANK + L10) ---  end  --- */

					/* --- strategy 44 (BRW + GAH + RANK Lower) + L20) --- start --- */
					if ($enable_robots[43] &&
						$player1_odd != NULL && $player2_odd != NULL &&
						// (($player1_odd >= 1.7 && $player1_odd <= 2) || ($player2_odd >= 1.7 && $player2_odd <= 2)) &&
						$player1_ranking != 501 && $player2_ranking != 501
					) {
						$player1_gah = 0;
						$player1_brw = 0;
						$i = 0;
						foreach ($player1_events as $player1_event) {
							$p_brws = json_decode($player1_event->p_brw);
							foreach ($p_brws as $p_brw) {
								$player1_brw += array_sum($p_brw);
							}
							$p_gahs = json_decode($player1_event->p_gah);
							foreach ($p_gahs as $p_gah) {
								$player1_gah += array_sum($p_gah);
							}
							$i ++;
							if ($i == 20) {
								break;
							}
						}
			
						$player2_gah = 0;
						$player2_brw = 0;
						$i = 0;
						foreach ($player2_events as $player2_event) {
							$p_brws = json_decode($player2_event->p_brw);
							foreach ($p_brws as $p_brw) {
								$player2_brw += array_sum($p_brw);
							}
							$p_gahs = json_decode($player2_event->p_gah);
							foreach ($p_gahs as $p_gah) {
								$player2_gah += array_sum($p_gah);
							}
							$i ++;
							if ($i == 20) {
								break;
							}
						}
						$expected_winner = ($player1_brw + $player1_gah) >= ($player2_brw + $player2_gah) ? 1 : 2;
						if (($expected_winner == 1 && $player1_ranking < $player2_ranking) || ($expected_winner == 2 && $player2_ranking < $player1_ranking)) {
							$insert_data = [
								"event_id" 			=> $event_id,
								"p1_brw" 			=> $player1_brw,
								"p2_brw" 			=> $player2_brw,
								"p1_gah" 			=> $player1_gah,
								"p2_gah" 			=> $player2_gah,
								"p1_rank" 			=> $player1_ranking,
								"p2_rank" 			=> $player2_ranking,
								"expected_winner" 	=> $expected_winner,
								"real_winner"		=> $real_winner,
							];
							DB::table($backtest_bots[43])
								->insert($insert_data);
						}
					}
					/* --- strategy 44 (BRW + GAH + RANK (Lower) + L20) ---  end  --- */

					$event_cnt ++;
					$log = "Ended events: " . $total_event_cnt . " / " . $event_cnt;
					Helper::printLog($log);
				}
			}
		}

	}
}
