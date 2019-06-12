import React from "react";
import {Table, Td, Tr} from "reactable";
import {connect} from "react-redux";

import HttpService from "../http/HttpService";
import ActionsUtils from "../actions/ActionsUtils";
import DateUtils from "../common/DateUtils";
import LoaderSpinner from "../components/Spinner.js";

import "../stylesheets/processes.styl";
import filterIcon from '../assets/img/search.svg'
import editIcon from '../assets/img/edit-icon.png'
import {withRouter} from 'react-router-dom'

class Archive extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      processes: [],
      filterVal: '',
      showLoader: true,
      showAddProcess: false,
      currentPage: 0,
      sort: { column: "name", direction: 1}
    };
  }

  componentDidMount() {
    const intervalIds = {
      reloadProcessesIntervalId: setInterval(() => this.reloadProcesses(), 10000)
    }
    this.setState(intervalIds)
    this.reloadProcesses();
  }

  componentWillUnmount() {
    if (this.state.reloadProcessesIntervalId) {
      clearInterval(this.state.reloadProcessesIntervalId)
    }
  }

  reloadProcesses() {
    HttpService.fetchArchivedProcesses().then ((fetchedProcesses) => {
      if (!this.state.showAddProcess) {
        this.setState({processes: fetchedProcesses, showLoader: false})
      }
    }).catch( e => this.setState({ showLoader: false }))
  }

  showProcess(process) {
    this.props.history.push(Archive.path + "/" + process.name)
  }

  handleChange(event) {
    this.setState({filterVal: event.target.value});
  }

  getFilterValue() {
    return this.state.filterVal.toLowerCase();
  }

  render() {
    return (
      <div className="Page">
        <div id="process-top-bar">
          <div id="table-filter" className="input-group">
            <input type="text" className="form-control" aria-describedby="basic-addon1"
                    value={this.state.filterVal} onChange={this.handleChange}/>
            <span className="input-group-addon" id="basic-addon1">
              <img id="search-icon" src={filterIcon} />
            </span>
          </div>
        </div>
        <LoaderSpinner show={this.state.showLoader}/>
        <Table className="esp-table"
               onSort={sort => this.setState({sort: sort})}
               onPageChange={currentPage => this.setState({currentPage: currentPage})}
           noDataText="No matching records found."
           hidden={this.state.showLoader}
           currentPage={this.state.currentPage}
           defaultSort={this.state.sort}
           itemsPerPage={10}
           pageButtonLimit={5}
           previousPageLabel="<"
           nextPageLabel=">"
           sortable={['name', 'category', 'modifyDate']}
           filterable={['name', 'category']}
           hideFilterInput
           filterBy={this.getFilterValue()}
           columns = {[
             {key: 'name', label: 'Process name'},
             {key: 'category', label: 'Category'},
             {key: 'modifyDate', label: 'Last modification'},
             {key: 'view', label: 'View'},
           ]}
        >

          {this.state.processes.map((process, index) => {
            return (
              <Tr className="row-hover" key={index}>
                <Td column="name">{process.name}</Td>
                <Td column="category">{process.processCategory}</Td>

                <Td column="modifyDate" className="date-column">{DateUtils.format(process.modificationDate)}</Td>
                <Td column="view" className="edit-column">
                  <img src={editIcon} className="edit-icon" title="View" onClick={e => this.showProcess(process)} />
                </Td>
              </Tr>
            )
          })}

        </Table>
      </div>
    )
  }
}

Archive.title = 'Archive Processes'
Archive.path = '/archivedProcesses'
Archive.header = 'Archive'

function mapState(state) {
  return {
    loggedUser: state.settings.loggedUser
  };
}

export default withRouter(connect(mapState, ActionsUtils.mapDispatchWithEspActions)(Archive));