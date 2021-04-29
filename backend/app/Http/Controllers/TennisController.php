<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use App\Helpers\Helper;

class TennisController extends Controller
{
    public function index() {
        echo "index";
    }

    /**
     * Get matches
     * @param   string  $date
     * @param   int     $type (0: upcoming, 1: inplay, 3: history)
     * @return  array   $matches
     */
    public function getMatches($date, $type) {
        $current_date = date('Y-m-d', time());
        if (!$date) {
            $date = $current_date;
        }
        $leagues = DB::table("f_league")->get();
        if ($type == 3) {
            $match_table_name = "f_matches_" . substr($date, 0, 4) . "_" . substr($date, 5, 2);
        } else if ($type == 0) {
            $match_table_name = "f_upcoming";
        } else if ($type == 1) {
            $match_table_name = "f_inplay";
        }
        if (!Schema::hasTable($match_table_name)) {
            return array();
        }
        $times = Helper::getTimePeriod($date);
        if ($type == 0) { // upcoming
            $matches_data = DB::table($match_table_name)
                            ->where('time_status', 0)
                            ->where('league_name', 'not like', "%Esoccer%")
                            ->orderBy('time')
                            ->get();
        } elseif ($type == 1) { // in play
            $matches_data = DB::table($match_table_name)
                            ->where('time_status', 1)
                            ->where('league_name', 'not like', "%Esoccer%")
                            ->orderBy('time')
                            ->get();
        } elseif ($type == 3) { // ended
            if ($date == $current_date) {
                $matches_data = DB::table($match_table_name)
                                ->where('time_status', 3)
                                ->where('scores', '<>', NULL)
                                ->where('scores', '<>', '')
                                ->where('league_name', 'not like', "%Esoccer%")
                                ->whereBetween('time', [$times[0], $times[1]])
                                ->orderBy('time')
                                ->get();
            } else {
                $matches_data = DB::table($match_table_name)
                                ->where('time_status', 3)
                                ->where('scores', '<>', NULL)
                                ->where('scores', '<>', '')
                                ->where('league_name', 'not like', "%Esoccer%")
                                ->whereBetween('time', [$times[0], $times[1]])
                                ->orderBy('time')
                                ->get();
            }
        }
        $matches_array = array();
        array_push($matches_array, $matches_data);
        return Helper::getMatchesResponse($matches_array, $leagues, $type);
    }

    /**
     * Get pre-calculated data for Upcoming & Inplay
     * @param  int   $player1_id
     * @param  int   $player2_id
     * @return array $matches
     */
    public function getRelationData($home_id, $away_id, $league_id, $event_id, $date) {
        $history_total_tables = DB::table("pg_catalog.pg_tables")
                                ->where("schemaname", "public")
                                ->where("tablename", "like", "f_matches_%")
                                ->get();

        $history_tables = array();
        $request_date = explode("-", $date);
        $request_year = (int)$request_date[0];
        $request_month = (int)$request_date[1];
        foreach ($history_total_tables as $table) {
            $table_dates = explode("_", $table->tablename);
            $year = (int)$table_dates[2];
            $month = (int)$table_dates[3];
            if ($year > $request_year || ($year == $request_year && $month <= $request_month)) {
                array_push($history_tables, $table);
            }
        }
        $leagues = DB::table("f_league")->get();
        $team_ids = [$home_id, $away_id];
        $relation_data = Helper::getRelationMatches($team_ids, $history_tables, $leagues, $league_id, $date);
        return $relation_data;
    }

    /**
     * History Request
     */
    public function history(Request $request) {
        try {
            $date = $request->input('date', date('Y-m-d', time()));
            $history_data = $this->getMatches($date, 3); // time_status (3: ended)
            $response = [
                "history_detail" => $history_data,
            ];
            return response()->json($response, 200);
        } catch (Exception $e) {
            return response()->json([], 500);
        }
    }

    /**
     * Upcoming Request
     */
    public function upComing(Request $request) {
        try {
            $upcoming_data = $this->getMatches(null, 0); // time_status (0: upcoming)
            $response = [
                "upcoming_detail" => $upcoming_data,
            ];
            return response()->json($response, 200);
        } catch (Exception $e) {
            return response()->json([], 500);
        }
    }

    public function inplayScoreUpdate(Request $request) {
        try {
            $current_scores = DB::table("t_inplay")
                                ->get();
            return response()->json($current_scores, 200);
        } catch (Exception $e) {
            return response()->json([], 500);
        }
    }

    public function inplay(Request $request) {
        try {
            $inplay_data = $this->getMatches(null, 1); // time_status (1: inplay)
            $response = [
                "inplay_detail" => $inplay_data,
            ];
            return response()->json($response, 200);
        } catch (Exception $e) {
            return response()->json([], 500);
        }
    }

    /**
     * Relation Request
     */
    public function relation(Request $request) {
        try {
            $home_id = $request->input('home_id', null);
            $away_id = $request->input('away_id', null);
            $league_id = $request->input('league_id', null);
            $event_id = $request->input('event_id', null);
            $date = $request->input('date', null);
            if (!$home_id || !$away_id || !$league_id || !$date || !$event_id) {
                return response()->json([], 200);
            }
            $relation_data = $this->getRelationData($home_id, $away_id, $league_id, $event_id, $date);
            return response()->json($relation_data, 200);
        } catch (Exception $e) {
            return response()->json([], 500);
        }
    }


    public function getTimer(Request $request) {
        try {
            $timer = DB::table("f_inplay_timer")
                        ->get();
            return response()->json($timer, 200);
        } catch (Exception $e) {
            return response()->json([], 500);
        }
    }

    public function robots(Request $request) {
        try {
            $robot_tables = [
                "t_backtest_bots_brw_gah_rank_10",
                "t_backtest_bots_brw_gah_rank_20",
            ];

            $robots = array();
            foreach ($robot_tables as $robot_table) {
                $robots[] = DB::table($robot_table)
                            ->select("event_id", "expected_winner", "real_winner")
                            ->get();    
            }
            return response()->json($robots, 200);
        } catch (Exception $e) {
            return response()->json([], 500);
        }
    }
}
