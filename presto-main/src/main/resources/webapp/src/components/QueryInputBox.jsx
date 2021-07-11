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

import {
    formatDataSizeBytes,
    formatShortTime,
    getHumanReadableState,
    getProgressBarPercentage,
    getProgressBarTitle,
    getQueryStateColor,
    GLYPHICON_DEFAULT,
    GLYPHICON_HIGHLIGHT,
    parseDataSize,
    parseDuration,
    truncateString
} from "../utils";
import {QueryListItem} from "./QueryList";

export class QueryInputBox extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            query_str: ''
        };

        this.handleChange = this.handleChange.bind(this);
        this.executeQuery = this.executeQuery.bind(this);
    }

    handleChange(event) {
        this.setState({
            query_str: event.target.value
        });
    }

    executeQuery(event) {
        // trim ';' and any white spaces from end of query string
        const query_str = this.state.query_str.replace(/[;\s]*$/, '');

        let requestOptions = {
            method: 'POST',
            headers: {
                'X-Presto-User': 'webui-tester',
                'X-Presto-Source': 'Gourd Store WebUI'
            },
            body: query_str
        };

        function followQuery(targetUrl) {
            fetch(targetUrl, requestOptions)
                .then(response => response.json())
                .then(function(json) {
                    if ('data' in json) {
                        console.log(json.data);
                    }
                    if ('nextUri' in json) {
                        if (requestOptions.method === 'POST') {
                            requestOptions.method = 'GET';
                            delete requestOptions.body;
                        }
                        followQuery(json.nextUri);
                    }
                })
                .catch(error => {
                    this.setState({ errorMessage: error.toString() });
                    console.error('There was an error!', error);
                });
        }

        followQuery('/v1/statement');
        this.setState({
            query_str: ''
        });
        event.preventDefault();
    }

    render() {
        return (
            <div>
                <div className="row toolbar-row">
                    <div className="col-xs-12 toolbar-col">
                        <div className="input-group input-group-sm">
                            <input type="text" className="form-control form-control-small search-bar"
                                   placeholder="Input your query here." onChange={this.handleChange} value={this.state.query_str} />
                            <div className="input-group-btn">
                                <button type="button" className="btn btn-default" onClick={this.executeQuery}>Submit</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}
