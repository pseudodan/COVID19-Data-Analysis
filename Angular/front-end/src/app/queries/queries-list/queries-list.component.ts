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
    new Query('Query 1', 'Query 1 description'),
    new Query('Query 2', 'Query 2 description')
  ];

  constructor() {}

  ngOnInit() {}

  // Function selects which query is selected from the menu.
  onQuerySelected(queries: Query){
    this.queryWasSelected.emit(queries);
  }
}
