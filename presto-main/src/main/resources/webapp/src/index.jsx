import React from "react";
import ReactDOM from "react-dom";
import {PageTitle} from "./components/PageTitle";
import {SparkInfo} from "./components/SparkInfo";
import {Comp} from "./components/Comparation";
import {QueryList} from "./components/QueryList";
import {ClusterHUD} from "./components/ClusterHUD";
import {PrestoQueryBox} from "./components/PrestoQueryBox";

ReactDOM.render(
    <PageTitle title="Gourd Store" />,
    document.getElementById('title')
);

class Main extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            presto_query_time: 0,
            spark_query_time: 0
        };
    }

    render() {
        return (
            <div>
                <div id="presto-info" className="col-sm-12 col-md-7">
                    <div className="panel panel-info">
                        <div className="panel-heading">Cluster Info</div>
                        <ClusterHUD />
                    </div>

                    <div className="panel panel-danger">
                        <div className="panel-heading">Gourd Store Query Box</div>
                        <PrestoQueryBox top_level={this} />
                    </div>
                </div>

                <div id="compare" className="col-sm-12 col-md-5">
                    <Comp father_node={this} />
                </div>

                <div id="spark-info" className="col-sm-12 col-md-5">
                    <SparkInfo father_node={this} />
                </div>
            </div>
        );
    }
}

ReactDOM.render(
    <Main />,
    document.getElementById("main")
)

ReactDOM.render(
    <QueryList />,
    document.getElementById('query-list')
);