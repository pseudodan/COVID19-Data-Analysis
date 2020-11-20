/* *** Query List Component ***
   Display the list of queries based on how many there are in the array.
    - queryWasSelected: Holds the event in which the user clicks on the query from the list of queries.
    - queries: Array of type Query from queries.model.ts that holds the name and description of every query.
    - onQuerySelected(): Pass the selected query from the list of queries to 'queryWasSelected' and emit it to the screen.
*/
import {Component, OnInit, EventEmitter, Output} from '@angular/core';
import { Query } from '../queries.model';

@Component({
  selector: 'app-queries-list',
  templateUrl: './queries-list.component.html',
  styleUrls: ['./queries-list.component.css']
})

export class QueriesListComponent implements OnInit{
  // Make listenable from the outside. (queries.component.html)
  @Output() queryWasSelected = new EventEmitter<Query>();
  // Array of queries using the query default constructor from queries.model
  queries: Query[] = [
    new Query('Largest number of positive, negative or inconclusive case outcomes by state.', 'Query 1 description'),
    new Query('Largest number of tests administered in a specified state.', 'Query 2 description'),
    new Query('Largest number of positive, negative or inconclusive case outcomes in an ordered list of states', 'Query 3 description'),
    new Query('Largest number of positive, negative or inconclusive cases in specified date range.', 'Query 4 description'),
    new Query('List of number of positive, negative or inconclusive cases per state.', 'Query 5 description')
  ];

  constructor() {}

  ngOnInit() {}

  // Function selects which query is selected from the menu.
  onQuerySelected(queries: Query){
    this.queryWasSelected.emit(queries);
  }
}
