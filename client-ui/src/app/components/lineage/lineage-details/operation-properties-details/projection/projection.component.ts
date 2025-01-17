/*
 * Copyright 2017 ABSA Group Limited
 *
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
import { Component } from '@angular/core';
import { PropertiesComponent } from '../properties/properties.component';
import { Property, PropertyType } from 'src/app/model/property';

@Component({
  selector: 'projection',
  templateUrl: './projection.component.html',
  styleUrls: ['./projection.component.less']
})
export class ProjectionComponent extends PropertiesComponent {

  public hasDroppedAttribute(properties: Property[]): boolean {
    return properties.filter(p => p.type == PropertyType.DroppedAttributes).length > 0
  }

}
