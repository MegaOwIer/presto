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
    getHumanReadableState,
    getProgressBarPercentage,
    getProgressBarTitle,
    getQueryStateColor,
    GLYPHICON_HIGHLIGHT,
    parseDataSize,
    parseDuration,
} from "../utils";

export class QueryListItem extends React.Component {
    renderWarning() {
        const query = this.props.query;
        if (query.warnings && query.warnings.length) {
            let warningCodes = [];
            query.warnings.forEach(function(warning) {
               warningCodes.push(warning.warningCode.name)
            });

            return (
                <span className="glyphicon glyphicon-warning-sign query-warning" data-toggle="tooltip" title={warningCodes.join(', ')}/>
            );
        }
    }

    render() {
        const query = this.props.query;
        const progressBarStyle = {width: getProgressBarPercentage(query) + "%", backgroundColor: getQueryStateColor(query)};

        const splitDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Completed splits">
                    <span className="glyphicon glyphicon-ok" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.completedDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Running splits">
                    <span className="glyphicon glyphicon-play" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.queryStats.runningDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Queued splits">
                    <span className="glyphicon glyphicon-pause" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.queryStats.queuedDrivers}
                    </span>
            </div>);

        // const timingDetails = (
        //     <div className="col-xs-12 tinystat-row">
        //         <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Wall time spent executing the query (not including queued time)">
        //             <span className="glyphicon glyphicon-hourglass" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
        //             {query.queryStats.executionTime}
        //         </span>
        //         <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Total query wall time">
        //             <span className="glyphicon glyphicon-time" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
        //             {query.queryStats.elapsedTime}
        //         </span>
        //         <span className="tinystat" data-toggle="tooltip" data-placement="top" title="CPU time spent by this query">
        //             <span className="glyphicon glyphicon-dashboard" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
        //             {query.queryStats.totalCpuTime}
        //         </span>
        //     </div>);

        const memoryDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Current reserved memory">
                    <span className="glyphicon glyphicon-scale" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.userMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Peak memory">
                    <span className="glyphicon glyphicon-fire" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.peakUserMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Cumulative user memory">
                    <span className="glyphicon glyphicon-equalizer" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDataSizeBytes(query.queryStats.cumulativeUserMemory / 1000.0)}
                </span>
            </div>);

        return (
            <div className="query">
                <div className="row query-header">
                    <div className="col-xs-12 query-progress-container">
                        <div className="progress">
                            <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={getProgressBarPercentage(query)} aria-valuemin="0"
                                 aria-valuemax="100" style={progressBarStyle}>
                                {getProgressBarTitle(query)}
                            </div>
                        </div>
                    </div>
                </div>

                <div className="row stat-row query-header query-header-queryid">
                    <div data-placement="bottom">
                        Query ID: &nbsp;&nbsp; <a href={"query.html?" + query.queryId} target="_blank" data-toggle="tooltip" title="Query ID">{query.queryId}</a>
                        {this.renderWarning()}
                    </div>
                </div>

                {/* Emphasise running time. */}
                <div className="row stat-row">
                    <div className="col-xs-6">
                        <span data-toggle="tooltip" data-placement="top" style={{fontSize: "24px"}}
                              title="Total query wall time">
                            &nbsp;&nbsp; Real Time: {query.queryStats.elapsedTime}
                        </span>
                    </div>
                    <div className="col-xs-6">
                        <span data-toggle="tooltip" data-placement="top" style={{fontSize: "24px"}}
                              title="CPU Time">
                            &nbsp;&nbsp; CPU Time: {query.queryStats.totalCpuTime}
                        </span>
                    </div>
                </div>

                <div className="row stat-row">
                    {splitDetails}
                </div>
                <div className="row stat-row">
                    {memoryDetails}
                </div>
            </div>
        );
    }
}

class DisplayedQueriesList extends React.Component {
    render() {
        const queryNodes = this.props.queries.map(function (query) {
            return (
                <QueryListItem key={query.queryId} query={query}/>
            );
        }.bind(this));
        return (
            <div>
                {queryNodes}
            </div>
        );
    }
}

const FILTER_TYPE = {
    RUNNING: function (query) {
        return !(query.state === "QUEUED" || query.state === "FINISHED" || query.state === "FAILED");
    },
    QUEUED: function (query) { return query.state === "QUEUED"},
    FINISHED: function (query) { return query.state === "FINISHED"},
};

const SORT_TYPE = {
    CREATED: function (query) {return Date.parse(query.queryStats.createTime)},
    ELAPSED: function (query) {return parseDuration(query.queryStats.elapsedTime)},
    EXECUTION: function (query) {return parseDuration(query.queryStats.executionTime)},
    CPU: function (query) {return parseDuration(query.queryStats.totalCpuTime)},
    CUMULATIVE_MEMORY: function (query) {return query.queryStats.cumulativeUserMemory},
    CURRENT_MEMORY: function (query) {return parseDataSize(query.queryStats.userMemoryReservation)},
};

const ERROR_TYPE = {
    USER_ERROR: function (query) {return query.state === "FAILED" && query.errorType === "USER_ERROR"},
    INTERNAL_ERROR: function (query) {return query.state === "FAILED" && query.errorType === "INTERNAL_ERROR"},
    INSUFFICIENT_RESOURCES: function (query) {return query.state === "FAILED" && query.errorType === "INSUFFICIENT_RESOURCES"},
    EXTERNAL: function (query) {return query.state === "FAILED" && query.errorType === "EXTERNAL"},
};

const SORT_ORDER = {
    ASCENDING: function (value) {return value},
    DESCENDING: function (value) {return -value}
};

export class PrestoQueryList extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            allQueries: [],
            displayedQueries: [],
            reorderInterval: 5000,
            currentSortType: SORT_TYPE.CREATED,
            currentSortOrder: SORT_ORDER.DESCENDING,
            stateFilters: [FILTER_TYPE.RUNNING, FILTER_TYPE.QUEUED, FILTER_TYPE.FINISHED],
            errorTypeFilters: [ERROR_TYPE.INTERNAL_ERROR, ERROR_TYPE.INSUFFICIENT_RESOURCES, ERROR_TYPE.EXTERNAL],
            searchString: props.searchString,
            maxQueries: 1,
            lastRefresh: Date.now(),
            lastReorder: Date.now(),
            initialized: false
        };

        this.refreshLoop = this.refreshLoop.bind(this);
        this.handleSearchStringChange = this.handleSearchStringChange.bind(this);
        this.executeSearch = this.executeSearch.bind(this);
        this.handleSortClick = this.handleSortClick.bind(this);
    }

    sortAndLimitQueries(queries, sortType, sortOrder, maxQueries) {
        queries.sort(function (queryA, queryB) {
            return sortOrder(sortType(queryA) - sortType(queryB));
        }, this);

        if (maxQueries !== 0 && queries.length > maxQueries) {
            queries.splice(maxQueries, (queries.length - maxQueries));
        }
    }

    filterQueries(queries, stateFilters, errorTypeFilters, searchString) {
        const stateFilteredQueries = queries.filter(function (query) {
            for (let i = 0; i < stateFilters.length; i++) {
                if (stateFilters[i](query)) {
                    return true;
                }
            }
            for (let i = 0; i < errorTypeFilters.length; i++) {
                if (errorTypeFilters[i](query)) {
                    return true;
                }
            }
            return false;
        });

        if (searchString === '') {
            return stateFilteredQueries;
        }
        else {
            return stateFilteredQueries.filter(function (query) {
                const term = searchString.toLowerCase();
                if (query.queryId.toLowerCase().indexOf(term) !== -1 ||
                    getHumanReadableState(query).toLowerCase().indexOf(term) !== -1 ||
                    query.query.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.session.user && query.session.user.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.session.source && query.session.source.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.resourceGroupId && query.resourceGroupId.join(".").toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                return query.warnings.some(function (warning) {
                    if ("warning".indexOf(term) !== -1 || warning.warningCode.name.toLowerCase().indexOf(term) !== -1 || warning.message.toLowerCase().indexOf(term) !== -1) {
                        return true;
                    }
                });

            }, this);
        }
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query === null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        clearTimeout(this.searchTimeoutId);

        $.get('/v1/query', function (queryList) {
            const queryMap = queryList.reduce(function (map, query) {
                map[query.queryId] = query;
                return map;
            }, {});

            let updatedQueries = [];
            this.state.displayedQueries.forEach(function (oldQuery) {
                if (oldQuery.queryId in queryMap) {
                    updatedQueries.push(queryMap[oldQuery.queryId]);
                    queryMap[oldQuery.queryId] = false;
                }
            });

            let newQueries = [];
            for (const queryId in queryMap) {
                if (queryMap[queryId]) {
                    newQueries.push(queryMap[queryId]);
                }
            }
            newQueries = this.filterQueries(newQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);

            const lastRefresh = Date.now();
            let lastReorder = this.state.lastReorder;

            if (this.state.reorderInterval !== 0 && ((lastRefresh - lastReorder) >= this.state.reorderInterval)) {
                updatedQueries = this.filterQueries(updatedQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
                updatedQueries = updatedQueries.concat(newQueries);
                this.sortAndLimitQueries(updatedQueries, this.state.currentSortType, this.state.currentSortOrder, 0);
                lastReorder = Date.now();
            }
            else {
                this.sortAndLimitQueries(newQueries, this.state.currentSortType, this.state.currentSortOrder, 0);
                updatedQueries = updatedQueries.concat(newQueries);
            }

            if (this.state.maxQueries !== 0 && (updatedQueries.length > this.state.maxQueries)) {
                updatedQueries.splice(this.state.maxQueries, (updatedQueries.length - this.state.maxQueries));
            }

            let query_time = 0;
            if (updatedQueries.length > 0) {
                var d1 = new Date(updatedQueries[0].queryStats.endTime)
                var d2 = new Date(updatedQueries[0].queryStats.createTime)
                query_time = d1 - d2
            }
            this.props.top_level.setState({
                presto_query_time: query_time
            });

            this.setState({
                allQueries: queryList,
                displayedQueries: updatedQueries,
                lastRefresh: lastRefresh,
                lastReorder: lastReorder,
                initialized: true
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

    handleSearchStringChange(event) {
        const newSearchString = event.target.value;
        clearTimeout(this.searchTimeoutId);

        this.setState({
            searchString: newSearchString
        });

        this.searchTimeoutId = setTimeout(this.executeSearch, 200);
    }

    executeSearch() {
        clearTimeout(this.searchTimeoutId);

        const newDisplayedQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(newDisplayedQueries, this.state.currentSortType, this.state.currentSortOrder, this.state.maxQueries);

        this.setState({
            displayedQueries: newDisplayedQueries
        });
    }

    renderMaxQueriesListItem(maxQueries, maxQueriesText) {
        return (
            <li><a href="#" className={this.state.maxQueries === maxQueries ? "selected" : ""} onClick={this.handleMaxQueriesClick.bind(this, maxQueries)}>{maxQueriesText}</a>
            </li>
        );
    }

    handleMaxQueriesClick(newMaxQueries) {
        const filteredQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder, newMaxQueries);

        this.setState({
            maxQueries: newMaxQueries,
            displayedQueries: filteredQueries
        });
    }

    renderReorderListItem(interval, intervalText) {
        return (
            <li><a href="#" className={this.state.reorderInterval === interval ? "selected" : ""} onClick={this.handleReorderClick.bind(this, interval)}>{intervalText}</a></li>
        );
    }

    handleReorderClick(interval) {
        if (this.state.reorderInterval !== interval) {
            this.setState({
                reorderInterval: interval,
            });
        }
    }

    renderSortListItem(sortType, sortText) {
        if (this.state.currentSortType === sortType) {
            const directionArrow = this.state.currentSortOrder === SORT_ORDER.ASCENDING ? <span className="glyphicon glyphicon-triangle-top"/> :
                <span className="glyphicon glyphicon-triangle-bottom"/>;
            return (
                <li>
                    <a href="#" className="selected" onClick={this.handleSortClick.bind(this, sortType)}>
                        {sortText} {directionArrow}
                    </a>
                </li>);
        }
        else {
            return (
                <li>
                    <a href="#" onClick={this.handleSortClick.bind(this, sortType)}>
                        {sortText}
                    </a>
                </li>);
        }
    }

    handleSortClick(sortType) {
        const newSortType = sortType;
        let newSortOrder = SORT_ORDER.DESCENDING;

        if (this.state.currentSortType === sortType && this.state.currentSortOrder === SORT_ORDER.DESCENDING) {
            newSortOrder = SORT_ORDER.ASCENDING;
        }

        const newDisplayedQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(newDisplayedQueries, newSortType, newSortOrder, this.state.maxQueries);

        this.setState({
            displayedQueries: newDisplayedQueries,
            currentSortType: newSortType,
            currentSortOrder: newSortOrder
        });
    }

    renderFilterButton(filterType, filterText) {
        let checkmarkStyle = {color: '#57aac7'};
        let classNames = "btn btn-sm btn-info style-check";
        if (this.state.stateFilters.indexOf(filterType) > -1) {
            classNames += " active";
            checkmarkStyle = {color: '#ffffff'};
        }

        return (
            <button type="button" className={classNames} onClick={this.handleStateFilterClick.bind(this, filterType)}>
                <span className="glyphicon glyphicon-ok" style={checkmarkStyle}/>&nbsp;{filterText}
            </button>
        );
    }

    handleStateFilterClick(filter) {
        const newFilters = this.state.stateFilters.slice();
        if (this.state.stateFilters.indexOf(filter) > -1) {
            newFilters.splice(newFilters.indexOf(filter), 1);
        }
        else {
            newFilters.push(filter);
        }

        const filteredQueries = this.filterQueries(this.state.allQueries, newFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder);

        this.setState({
            stateFilters: newFilters,
            displayedQueries: filteredQueries
        });
    }

    renderErrorTypeListItem(errorType, errorTypeText) {
        let checkmarkStyle = {color: '#ffffff'};
        if (this.state.errorTypeFilters.indexOf(errorType) > -1) {
            checkmarkStyle = GLYPHICON_HIGHLIGHT;
        }
        return (
            <li>
                <a href="#" onClick={this.handleErrorTypeFilterClick.bind(this, errorType)}>
                    <span className="glyphicon glyphicon-ok" style={checkmarkStyle}/>
                    &nbsp;{errorTypeText}
                </a>
            </li>);
    }

    handleErrorTypeFilterClick(errorType) {
        const newFilters = this.state.errorTypeFilters.slice();
        if (this.state.errorTypeFilters.indexOf(errorType) > -1) {
            newFilters.splice(newFilters.indexOf(errorType), 1);
        }
        else {
            newFilters.push(errorType);
        }

        const filteredQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, newFilters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder);

        this.setState({
            errorTypeFilters: newFilters,
            displayedQueries: filteredQueries
        });
    }

    render() {
        let queryList = <DisplayedQueriesList queries={this.state.displayedQueries}/>;
        if (this.state.displayedQueries === null || this.state.displayedQueries.length === 0) {
            let label = "No queries from WebUI";
            queryList = (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>{label}</h4></div>
                </div>
            );
        }

        return (
            <div>
                {queryList}
            </div>
        );
    }
}
