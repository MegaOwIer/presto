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
import {ClusterHUD} from "./ClusterHUD";
import {PrestoQueryBox} from "./PrestoQueryBox";

export class PrestoInfo extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
        <div>
            <div className="panel panel-info">
                <div className="panel-heading">Presto Cluster Info</div>
                <ClusterHUD />
            </div>

            <div className="panel panel-warning">
                <div className="panel-heading">Presto Query Box</div>
                <PrestoQueryBox top_level={this.props.father_node} />
            </div>
        </div>);
    }
}