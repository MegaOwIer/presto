/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from "react";
import {SparkQueryInputBox} from "./SparkQueryInputBox";
import {PrestoQueryList} from "./PrestoQueryList";

export class SparkInfo extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            driverID: '',
            appID: '',
            duration: 0,
            initialized: false
        };

        this.refreshLoop = this.refreshLoop.bind(this);
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        this.timeoutId = setTimeout(this.refreshLoop, 1000);
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously

        $.get('http://112.126.79.236:8001/json', function (json) {
            // apps
            let appID = "-1";
            let duration = 0;
            if (json["activeapps"].length > 0) {
                appID = json["activeapps"][json["activeapps"].length - 1].id;
                duration = json["activeapps"][json["activeapps"].length - 1].duration;
            } else if (json["completedapps"].length > 0) {
                appID = json["completedapps"][json["completedapps"].length - 1].id;
                duration = json["completedapps"][json["completedapps"].length - 1].duration;
            } else {
                appID = "";
                duration = 0;
            }

            // drivers
            let driverID = "-1";
            if (json["activedrivers"].length > 0) {
                driverID = json["activedrivers"][json["activedrivers"].length - 1].id;
            } else if (json["completeddrivers"].length > 0) {
                driverID = json["completeddrivers"][json["completeddrivers"].length - 1].id;
            } else {
                driverID = "";
            }

            this.setState({
                driverID: driverID,
                appID: appID,
                duration: duration,
                initialized: true
            });
            this.props.father_node.setState({
                spark_query_time: duration
            });
            this.resetTimer();
        }.bind(this))
            .error(function () {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            }.bind(this));
    }

    componentDidMount() {
        this.refreshLoop();
    }

    render() {
        return (
        <div>
            <div className="panel panel-warning">
                <div className="panel-heading">Spark Query Box</div>
                {/*<div className="panel-body">*/}
                {/*    <SparkQueryInputBox father_node={this} />*/}
                {/*</div>*/}
                <div className="panel-footer">
                    <div className="tabbable">
                        <ul className="nav nav-tabs">
                            <li className="active"><a href="#spark-details" data-toggle="tab">Details</a></li>
                            <li><a href="#spark-code" data-toggle="tab">Query Code</a></li>
                        </ul>
                        <div className="tab-content">
                            <div className="tab-pane active" id="spark-details">
                                <div className="row stat-row query-header query-header-queryid">
                                    <div data-placement="bottom">
                                        Driver ID: &nbsp;&nbsp; {this.state.driverID}
                                    </div>
                                    <div data-placement="bottom">
                                        Application ID: &nbsp;&nbsp; {this.state.appID}
                                    </div>
                                </div>

                                <div className="row stat-row">
                                    <div className="col-xs-12">
                                        <span data-toggle="tooltip" data-placement="top" style={{fontSize: "24px"}}
                                              title="Total query wall time">
                                            &nbsp;&nbsp; Real Time: {this.state.duration / 1000} s
                                        </span>
                                    </div>
                                </div>
                            </div>
                            <div className="tab-pane" id="spark-code">
                                <pre style={{minHeight: "150px"}}>
                                    Query Code.
                                </pre>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>);
    }
}