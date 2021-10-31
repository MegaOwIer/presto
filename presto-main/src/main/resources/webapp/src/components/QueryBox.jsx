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

import {QueryInputBox} from "./QueryInputBox";
import {QueryList} from "./QueryList";

export class QueryBox extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            lastQueryStr: '',
            result_columns: [],
            result_data: []
        };
    }

    render() {
        return (
            <div>
                <div className="panel-body">
                    <QueryInputBox father_node={this} />
                </div>
                <div className="panel-footer">
                    <div className="tabbable">
                        <ul className="nav nav-tabs">
                            <li className="active"><a href="#presto-details" data-toggle="tab">Details</a></li>
                            <li><a href="#presto-code" data-toggle="tab">Query Code</a></li>
                            <li><a href="#presto-result" data-toggle="tab">Result</a></li>
                        </ul>
                        <div className="tab-content">
                            <div className="tab-pane active" id="presto-details">
                                <QueryList father_node={this} searchString="Gourd Store WebUI" />
                            </div>
                            <div className="tab-pane" id="presto-code">
                                <pre style={{minHeight: "150px"}}>
                                    { this.state.lastQueryStr }
                                </pre>
                            </div>
                            <div className="tab-pane" id="presto-result">
                                <pre style={{minHeight: "150px"}}>
                                    { this.state.result_columns.join(' | ') + "\n" }
                                    { this.state.result_data.join('\n') }
                                </pre>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}
