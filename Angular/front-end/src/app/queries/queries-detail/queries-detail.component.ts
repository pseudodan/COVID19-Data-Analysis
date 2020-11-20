/* *** Queries Detail Component ***
*  Data is bound to the variable 'query' which contains information about each query.
*   - query: Type Query that holds information about each query. The model for it is in
*            queries.model.ts
*/
import {Component, Input} from '@angular/core';
import { Query } from '../queries.model';
@Component({
  selector: 'app-queries-detail',
  templateUrl: './queries-detail.component.html',
  styleUrls: ['./queries-detail.component.css']
})
export class QueriesDetailComponent{
  // May bind data to it from outside. Uses the queries default constructor from queries.model.
  @Input() query: Query;
}
